import asyncio
import logging
import httpx
from typing import List, Optional, Tuple
from dataclasses import dataclass, field
from tortoise.transactions import in_transaction

from models.expert import Expert
from models.availability_error import AvailabilityError
from services.cronofy_service import CronofyService, CronofyAPIError
from services.algolia_service import algolia_service
from config.settings import settings
from core.logging_utils import get_structured_logger
from schemas.availability import AvailabilityResult, AvailabilityData

logger = logging.getLogger(__name__)
structured_logger = get_structured_logger(__name__)


@dataclass
class BatchResults:
    """Results from processing a batch of experts"""
    processed: int = 0
    failed: int = 0
    algolia_updates: List[dict] = field(default_factory=list)

    def merge(self, other: 'BatchResults'):
        """Merge another BatchResults into this one"""
        self.processed += other.processed
        self.failed += other.failed
        self.algolia_updates.extend(other.algolia_updates)


class ExpertService:
    """Service for expert business logic"""

    # -------------------------------------------------------------------------
    # Public Methods
    # -------------------------------------------------------------------------

    @staticmethod
    async def bulk_upsert_experts(expert_data: List[dict]) -> int:
        """Bulk upsert experts with transaction support"""
        if not expert_data:
            return 0

        try:
            async with in_transaction():
                existing_bubble_uids = {expert["bubble_uid"] for expert in expert_data}
                existing_experts = await Expert.filter(bubble_uid__in=list(existing_bubble_uids)).all()
                existing_map = {expert.bubble_uid: expert for expert in existing_experts}

                experts_to_create = []
                experts_to_update = []

                for item in expert_data:
                    bubble_uid = item["bubble_uid"]
                    if bubble_uid in existing_map:
                        existing_expert = existing_map[bubble_uid]
                        existing_expert.expert_name = item["expert_name"]
                        existing_expert.cronofy_id = item["cronofy_id"]
                        existing_expert.calendar_ids = item["calendar_ids"]
                        existing_expert.version += 1
                        experts_to_update.append(existing_expert)
                    else:
                        experts_to_create.append(Expert(
                            expert_name=item["expert_name"],
                            cronofy_id=item["cronofy_id"],
                            calendar_ids=item["calendar_ids"],
                            bubble_uid=item["bubble_uid"],
                            version=0
                        ))

                if experts_to_create:
                    await Expert.bulk_create(experts_to_create)

                if experts_to_update:
                    await Expert.bulk_update(
                        experts_to_update,
                        fields=['expert_name', 'cronofy_id', 'calendar_ids', 'updated_at', 'version']
                    )

                updated_count = len(experts_to_create) + len(experts_to_update)
                structured_logger.info(
                    "Bulk expert upsert completed",
                    total_processed=updated_count,
                    created_count=len(experts_to_create),
                    updated_count=len(experts_to_update),
                    operation="bulk_upsert"
                )
                return updated_count

        except Exception as e:
            logger.error(f"Transaction failed during bulk expert upsert: {str(e)}")
            raise

    @staticmethod
    async def get_all_experts_with_data() -> List[dict]:
        """Get all experts with formatted data"""
        experts = await Expert.get_all_ordered()
        return [
            {
                "expert_name": expert.expert_name,
                "cronofy_id": expert.cronofy_id,
                "calendar_ids": expert.calendar_ids,
                "bubble_uid": expert.bubble_uid,
                "created_at": expert.created_at.isoformat(),
                "updated_at": expert.updated_at.isoformat(),
                "last_availability_check": expert.last_availability_check.isoformat() if expert.last_availability_check else None,
                "earliest_available_unix": expert.earliest_available_unix
            }
            for expert in experts
        ]

    @staticmethod
    async def update_all_expert_availability(
            duration: int = 60,
            buffer_before: int = 0,
            buffer_after: int = 0,
            days_ahead: int = 30
    ):
        """
        Fetch availability for all experts and update database/Algolia.

        Processes experts in batches. If a batch fails with 422 (usually caused
        by one bad expert poisoning the batch), falls back to individual processing.
        """
        try:
            experts = await Expert.get_all_ordered()
            if not experts:
                logger.info("No experts found in database")
                return

            logger.info(f"Updating availability for {len(experts)} experts")

            batches = CronofyService.batch_experts(experts, batch_size=10)
            results = BatchResults()
            fetch_params = (duration, buffer_before, buffer_after, days_ahead)

            for batch_idx, batch in enumerate(batches):
                batch_num = batch_idx + 1
                total_batches = len(batches)
                logger.info(f"Processing batch {batch_num}/{total_batches} with {len(batch)} experts")

                batch_results = await ExpertService._process_batch(
                    batch, batch_num, fetch_params
                )
                results.merge(batch_results)

                # Rate limit between batches
                if batch_idx < len(batches) - 1:
                    await asyncio.sleep(0.5)

            # Sync to Algolia
            await algolia_service.update_expert_records(results.algolia_updates)
            logger.info(f"Processing complete. Processed: {results.processed}, Failed: {results.failed}")

        except Exception as e:
            logger.error(f"Error in update_all_expert_availability: {str(e)}")

    # -------------------------------------------------------------------------
    # Batch Processing Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    async def _process_batch(
            batch: List[Expert],
            batch_num: int,
            fetch_params: Tuple[int, int, int, int]
    ) -> BatchResults:
        """
        Process a single batch of experts.
        Falls back to individual processing if batch fails with 422.
        """
        duration, buffer_before, buffer_after, days_ahead = fetch_params

        try:
            availability_results = await CronofyService.fetch_experts_availability_batch(
                batch,
                duration=duration,
                buffer_before=buffer_before,
                buffer_after=buffer_after,
                days_ahead=days_ahead
            )

            # Check for batch-level 422 failure (one bad expert poisons the batch)
            if ExpertService._is_batch_422_failure(availability_results):
                logger.info(f"Batch {batch_num} had all 422 errors - triggering individual fallback")
                return await ExpertService._process_experts_individually(
                    batch, batch_num, fetch_params
                )

            # Process normal batch results
            return await ExpertService._process_batch_results(
                batch, availability_results, batch_num
            )

        except CronofyAPIError as e:
            return await ExpertService._handle_batch_api_error(
                e, batch, batch_num, fetch_params
            )

        except httpx.HTTPStatusError as e:
            return await ExpertService._handle_batch_server_error(e, batch)

        except Exception as e:
            return await ExpertService._handle_batch_unexpected_error(e, batch)

    @staticmethod
    def _is_batch_422_failure(results: List[AvailabilityResult]) -> bool:
        """
        Check if all results failed with 422 error.
        This indicates one bad expert caused the entire batch to fail.
        """
        if len(results) <= 1:
            return False
        return all(
            not r.success and r.error_reason and "422" in r.error_reason
            for r in results
        )

    @staticmethod
    async def _process_batch_results(
            batch: List[Expert],
            results: List[AvailabilityResult],
            batch_num: int
    ) -> BatchResults:
        """Process results from a successful batch API call"""
        batch_results = BatchResults()

        for expert, result in zip(batch, results):
            try:
                if result.success:
                    algolia_record = await ExpertService._handle_expert_success(
                        expert, result.availability_data, batch_num
                    )
                    batch_results.algolia_updates.append(algolia_record)
                    batch_results.processed += 1
                else:
                    await ExpertService._handle_expert_failure(
                        expert, result.error_reason, result.error_details, batch_num
                    )
                    batch_results.failed += 1

            except Exception as e:
                await ExpertService._handle_expert_processing_error(expert, e, batch_num)
                batch_results.failed += 1

        return batch_results

    @staticmethod
    async def _process_experts_individually(
            experts: List[Expert],
            batch_num: int,
            fetch_params: Tuple[int, int, int, int]
    ) -> BatchResults:
        """
        Process each expert individually.
        Used as fallback when batch processing fails with 422.
        """
        duration, buffer_before, buffer_after, days_ahead = fetch_params
        results = BatchResults()

        for expert in experts:
            try:
                individual_results = await CronofyService.fetch_experts_availability_batch(
                    [expert],
                    duration=duration,
                    buffer_before=buffer_before,
                    buffer_after=buffer_after,
                    days_ahead=days_ahead
                )
                result = individual_results[0]

                if result.success:
                    algolia_record = await ExpertService._handle_expert_success(
                        expert, result.availability_data, batch_num, is_fallback=True
                    )
                    results.algolia_updates.append(algolia_record)
                    results.processed += 1
                else:
                    await ExpertService._handle_expert_failure(
                        expert, result.error_reason, result.error_details, batch_num
                    )
                    results.failed += 1

                # Small delay between individual requests
                await asyncio.sleep(0.2)

            except Exception as e:
                logger.error(f"Individual fallback failed for {expert.expert_name}: {e}")
                await AvailabilityError.log_error(
                    bubble_uid=expert.bubble_uid,
                    expert_name=expert.expert_name,
                    cronofy_id=expert.cronofy_id,
                    error_reason="Individual fallback error",
                    error_details=str(e)
                )
                results.failed += 1

        logger.info(f"Individual fallback for batch {batch_num}: {results.processed} success, {results.failed} failed")
        return results

    # -------------------------------------------------------------------------
    # Success/Failure Handlers
    # -------------------------------------------------------------------------

    @staticmethod
    async def _handle_expert_success(
            expert: Expert,
            availability: AvailabilityData,
            batch_num: int,
            is_fallback: bool = False
    ) -> dict:
        """Handle successful availability update for an expert"""
        await AvailabilityError.clear_error(expert.bubble_uid)

        old_timestamp = expert.earliest_available_unix
        new_timestamp = availability.earliest_available_unix

        log_msg = "Individual fallback success" if is_fallback else "Expert availability update - SUCCESS"
        structured_logger.info(
            log_msg,
            expert_name=expert.expert_name,
            bubble_uid=expert.bubble_uid,
            cronofy_id=expert.cronofy_id,
            old_timestamp=old_timestamp,
            new_timestamp=new_timestamp,
            timestamp_changed=old_timestamp != new_timestamp,
            batch_index=batch_num
        )

        await expert.update_availability(new_timestamp)
        return ExpertService._build_algolia_record(expert, availability)

    @staticmethod
    async def _handle_expert_failure(
            expert: Expert,
            error_reason: str,
            error_details: str,
            batch_num: int
    ):
        """Handle failed availability check for an expert"""
        structured_logger.error(
            "Expert availability check failed",
            expert_name=expert.expert_name,
            bubble_uid=expert.bubble_uid,
            cronofy_id=expert.cronofy_id,
            error_reason=error_reason,
            error_details=error_details,
            batch_index=batch_num
        )

        await AvailabilityError.log_error(
            bubble_uid=expert.bubble_uid,
            expert_name=expert.expert_name,
            cronofy_id=expert.cronofy_id,
            error_reason=error_reason,
            error_details=error_details
        )

    @staticmethod
    async def _handle_expert_processing_error(expert: Expert, error: Exception, batch_num: int):
        """Handle unexpected error while processing an expert's result"""
        structured_logger.error(
            "Failed to process expert availability",
            expert_name=expert.expert_name,
            bubble_uid=expert.bubble_uid,
            cronofy_id=expert.cronofy_id,
            error=str(error),
            error_type=type(error).__name__,
            batch_index=batch_num
        )

        await AvailabilityError.log_error(
            bubble_uid=expert.bubble_uid,
            expert_name=expert.expert_name,
            cronofy_id=expert.cronofy_id,
            error_reason="Processing error",
            error_details=f"{type(error).__name__}: {str(error)}"
        )

    # -------------------------------------------------------------------------
    # Batch Error Handlers
    # -------------------------------------------------------------------------

    @staticmethod
    async def _handle_batch_api_error(
            error: CronofyAPIError,
            batch: List[Expert],
            batch_num: int,
            fetch_params: Tuple[int, int, int, int]
    ) -> BatchResults:
        """Handle 4xx API errors at the batch level"""
        logger.error(f"Failed to process batch {batch_num}: {str(error)}")

        # 422 errors: try individual fallback
        if error.status_code == 422:
            logger.info(f"Attempting individual fallback for batch {batch_num} due to 422 error")
            return await ExpertService._process_experts_individually(
                batch, batch_num, fetch_params
            )

        # Other 4xx errors: log for all experts
        for expert in batch:
            await AvailabilityError.log_error(
                bubble_uid=expert.bubble_uid,
                expert_name=expert.expert_name,
                cronofy_id=expert.cronofy_id,
                error_reason=f"API error ({error.status_code})",
                error_details=f"Entire batch failed: {str(error)}"
            )

        return BatchResults(failed=len(batch))

    @staticmethod
    async def _handle_batch_server_error(
            error: httpx.HTTPStatusError,
            batch: List[Expert]
    ) -> BatchResults:
        """Handle 5xx server errors at the batch level"""
        logger.error(f"Server error processing batch: {str(error)}")

        for expert in batch:
            await AvailabilityError.log_error(
                bubble_uid=expert.bubble_uid,
                expert_name=expert.expert_name,
                cronofy_id=expert.cronofy_id,
                error_reason=f"Server error ({error.response.status_code})",
                error_details=f"Entire batch failed after retries: {str(error)}"
            )

        return BatchResults(failed=len(batch))

    @staticmethod
    async def _handle_batch_unexpected_error(
            error: Exception,
            batch: List[Expert]
    ) -> BatchResults:
        """Handle unexpected errors at the batch level"""
        logger.error(f"Unexpected error processing batch: {str(error)}")

        for expert in batch:
            await AvailabilityError.log_error(
                bubble_uid=expert.bubble_uid,
                expert_name=expert.expert_name,
                cronofy_id=expert.cronofy_id,
                error_reason="Batch processing error",
                error_details=f"Unexpected error: {type(error).__name__}: {str(error)}"
            )

        return BatchResults(failed=len(batch))

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    @staticmethod
    def _build_algolia_record(expert: Expert, availability: AvailabilityData) -> dict:
        """Build Algolia record for an expert (single source of truth)"""
        return {
            "objectID": expert.bubble_uid,
            "expert_name": expert.expert_name,
            "cronofy_id": expert.cronofy_id,
            "earliest_available_unix": availability.earliest_available_unix,
            "availability_last_updated": availability.last_updated
        }
