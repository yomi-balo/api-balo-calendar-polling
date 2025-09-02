import asyncio
import logging
from typing import List
from tortoise.transactions import in_transaction

from models.expert import Expert
from services.cronofy_service import CronofyService
from services.algolia_service import algolia_service
from config.settings import settings

logger = logging.getLogger(__name__)


class ExpertService:
    """Service for expert business logic"""

    @staticmethod
    async def bulk_upsert_experts(expert_data: List[dict]) -> int:
        """Bulk upsert experts with transaction support"""
        if not expert_data:
            return 0

        try:
            async with in_transaction():
                updated_count = 0
                for expert in expert_data:
                    await Expert.upsert(
                        expert["expert_name"],
                        expert["cronofy_id"],
                        expert["calendar_ids"],
                        expert["bubble_uid"]
                    )
                    updated_count += 1

                logger.info(f"Upserted {updated_count} expert calendar mappings to database")
                return updated_count

        except Exception as e:
            logger.error(f"Transaction failed during bulk expert upsert: {str(e)}")
            raise

    @staticmethod
    async def get_all_experts_with_data() -> List[dict]:
        """Get all experts with formatted data"""
        experts = await Expert.get_all_ordered()

        expert_data = []
        for expert in experts:
            expert_data.append({
                "expert_name": expert.expert_name,
                "cronofy_id": expert.cronofy_id,
                "calendar_ids": expert.calendar_ids,
                "bubble_uid": expert.bubble_uid,
                "created_at": expert.created_at.isoformat(),
                "updated_at": expert.updated_at.isoformat(),
                "last_availability_check": expert.last_availability_check.isoformat() if expert.last_availability_check else None,
                "earliest_available_unix": expert.earliest_available_unix
            })

        return expert_data

    @staticmethod
    async def update_all_expert_availability(
            duration: int = 60,
            buffer_before: int = 0,
            buffer_after: int = 0,
            days_ahead: int = 30
    ):
        """Fetch availability for all experts from database in batches and update Algolia"""
        try:
            experts = await Expert.get_all_ordered()

            if not experts:
                logger.info("No experts found in database")
                return

            logger.info(f"Updating availability for {len(experts)} experts using new Cronofy slots API")

            algolia_updates = []

            # Use new batching method - max 15 experts per batch
            expert_batches = CronofyService.batch_experts(experts, batch_size=10)
            total_processed = 0
            total_failed = 0

            for batch_idx, expert_batch in enumerate(expert_batches):
                try:
                    logger.info(
                        f"Processing batch {batch_idx + 1}/{len(expert_batches)} with {len(expert_batch)} experts")

                    # Use new availability API with configurable parameters
                    availability_results = await CronofyService.fetch_experts_availability_batch(
                        expert_batch,
                        duration=duration,
                        buffer_before=buffer_before,
                        buffer_after=buffer_after,
                        days_ahead=days_ahead
                    )

                    for expert, availability in zip(expert_batch, availability_results):
                        try:
                            await expert.update_availability(availability.earliest_available_unix)

                            algolia_record = {
                                "objectID": expert.bubble_uid,  # Use bubble_uid as Algolia objectID
                                "expert_name": expert.expert_name,
                                "cronofy_id": expert.cronofy_id,
                                "earliest_available_unix": availability.earliest_available_unix,
                                "availability_last_updated": availability.last_updated
                            }

                            algolia_updates.append(algolia_record)
                            total_processed += 1

                        except Exception as e:
                            logger.error(
                                f"Failed to process expert {expert.expert_name} ({expert.bubble_uid}): {str(e)}")
                            total_failed += 1

                    # Add delay between batches to be respectful to API
                    if batch_idx < len(expert_batches) - 1:
                        await asyncio.sleep(0.5)

                except Exception as e:
                    logger.error(f"Failed to process batch {batch_idx + 1}: {str(e)}")
                    total_failed += len(expert_batch)

            # Update Algolia
            await algolia_service.update_expert_records(algolia_updates)

            logger.info(f"Processing complete. Processed: {total_processed}, Failed: {total_failed}")

        except Exception as e:
            logger.error(f"Error in update_all_expert_availability: {str(e)}")