import asyncio
import logging
from typing import List

from models.expert import Expert
from services.cronofy_service import CronofyService
from services.algolia_service import algolia_service
from config.settings import settings

logger = logging.getLogger(__name__)


class ExpertService:
    """Service for expert business logic"""

    @staticmethod
    async def bulk_upsert_experts(expert_data: List[dict]) -> int:
        """Bulk upsert experts"""
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
    def _create_expert_batches(experts: List[Expert]) -> List[List[Expert]]:
        """Create smart batches based on calendar count"""
        expert_batches = []
        current_batch = []
        current_calendar_count = 0

        for expert in experts:
            expert_calendar_count = len(expert.calendar_ids)

            if current_calendar_count + expert_calendar_count > settings.CRONOFY_MAX_CALENDARS_PER_REQUEST:
                if current_batch:
                    expert_batches.append(current_batch)
                current_batch = [expert]
                current_calendar_count = expert_calendar_count
            else:
                current_batch.append(expert)
                current_calendar_count += expert_calendar_count

        if current_batch:
            expert_batches.append(current_batch)

        return expert_batches

    @staticmethod
    async def update_all_expert_availability():
        """Fetch availability for all experts from database in batches and update Algolia"""
        try:
            experts = await Expert.get_all_ordered()

            if not experts:
                logger.info("No experts found in database")
                return

            logger.info(f"Updating availability for {len(experts)} experts from database using smart batching")

            algolia_updates = []
            expert_batches = ExpertService._create_expert_batches(experts)
            total_processed = 0
            total_failed = 0

            for batch_idx, expert_batch in enumerate(expert_batches):
                try:
                    batch_calendar_count = sum(len(expert.calendar_ids) for expert in expert_batch)
                    logger.info(
                        f"Processing batch {batch_idx + 1}/{len(expert_batches)} with {len(expert_batch)} experts "
                        f"and {batch_calendar_count} calendar IDs")

                    availability_results = await CronofyService.fetch_experts_availability_batch(expert_batch)

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