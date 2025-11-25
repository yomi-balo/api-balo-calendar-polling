import asyncio
import logging
from typing import List

from models.availability_error import AvailabilityError
from models.expert import Expert
from services.cronofy_service import CronofyService
from services.algolia_service import algolia_service
from config.settings import settings
from core.logging_utils import get_structured_logger

logger = logging.getLogger(__name__)
structured_logger = get_structured_logger(__name__)


class ErrorRetryService:
    """Service for retrying experts with availability errors"""

    @staticmethod
    async def retry_failed_experts(min_age_minutes: int = None) -> dict:
        """
        Retry availability checks for experts in the AvailabilityError table
        
        Args:
            min_age_minutes: Minimum age in minutes before retrying (defaults to config setting)
            
        Returns:
            Dictionary with retry statistics
        """
        if min_age_minutes is None:
            min_age_minutes = settings.ERROR_RETRY_MIN_AGE_MINUTES
            
        try:
            # Get experts that are ready for retry
            error_records = await AvailabilityError.get_errors_ready_for_retry(min_age_minutes)
            
            if not error_records:
                logger.info("No experts ready for retry at this time")
                return {
                    "total_ready_for_retry": 0,
                    "successful_retries": 0,
                    "failed_retries": 0,
                    "processing_errors": 0
                }
            
            logger.info(f"Found {len(error_records)} experts ready for retry")
            
            successful_retries = 0
            failed_retries = 0
            processing_errors = 0
            algolia_updates = []
            
            # Process each expert individually (sequential, not batched)
            for error_record in error_records:
                try:
                    # Get the expert record
                    expert = await Expert.get_by_bubble_uid(error_record.bubble_uid)
                    if not expert:
                        logger.warning(f"Expert not found for bubble_uid {error_record.bubble_uid}, skipping retry")
                        # Remove the orphaned error record
                        await AvailabilityError.clear_error(error_record.bubble_uid)
                        continue
                    
                    structured_logger.info(
                        "Retrying expert availability check",
                        expert_name=expert.expert_name,
                        bubble_uid=expert.bubble_uid,
                        cronofy_id=expert.cronofy_id,
                        last_error_reason=error_record.error_reason,
                        error_age_minutes=min_age_minutes
                    )
                    
                    # Fetch fresh availability data using the same logic as manual refresh
                    availability = await CronofyService.fetch_expert_availability(
                        expert.cronofy_id, expert.calendar_ids
                    )
                    
                    # Check if there was an error in the availability response
                    if availability and hasattr(availability, 'error') and availability.error:
                        # Log updated error to availability_errors table
                        await AvailabilityError.log_error(
                            bubble_uid=expert.bubble_uid,
                            expert_name=expert.expert_name,
                            cronofy_id=expert.cronofy_id,
                            error_reason=availability.error,
                            error_details=availability.error_details
                        )
                        
                        structured_logger.warning(
                            "Expert retry failed - still has availability error",
                            expert_name=expert.expert_name,
                            bubble_uid=expert.bubble_uid,
                            error_reason=availability.error,
                            error_details=availability.error_details
                        )
                        failed_retries += 1
                    else:
                        # Success - clear any existing error and update database
                        await AvailabilityError.clear_error(expert.bubble_uid)
                        
                        if availability:
                            old_timestamp = expert.earliest_available_unix
                            await expert.update_availability(availability.earliest_available_unix)
                            
                            # Prepare Algolia update
                            algolia_record = {
                                "objectID": expert.bubble_uid,
                                "expert_name": expert.expert_name,
                                "cronofy_id": expert.cronofy_id,
                                "earliest_available_unix": availability.earliest_available_unix,
                                "availability_last_updated": availability.last_updated
                            }
                            algolia_updates.append(algolia_record)
                            
                            structured_logger.info(
                                "Expert retry successful - availability updated",
                                expert_name=expert.expert_name,
                                bubble_uid=expert.bubble_uid,
                                old_timestamp=old_timestamp,
                                new_timestamp=availability.earliest_available_unix,
                                timestamp_changed=old_timestamp != availability.earliest_available_unix
                            )
                        
                        successful_retries += 1
                    
                    # Add small delay between individual retries to be respectful
                    await asyncio.sleep(0.2)
                    
                except Exception as e:
                    logger.error(f"Processing error during retry for expert {error_record.bubble_uid}: {str(e)}")
                    
                    # Log processing error to availability_errors table
                    await AvailabilityError.log_error(
                        bubble_uid=error_record.bubble_uid,
                        expert_name=error_record.expert_name,
                        cronofy_id=error_record.cronofy_id,
                        error_reason="Retry processing error",
                        error_details=f"{type(e).__name__}: {str(e)}"
                    )
                    processing_errors += 1
            
            # Update Algolia with all successful retries
            if algolia_updates:
                await algolia_service.update_expert_records(algolia_updates)
                logger.info(f"Updated {len(algolia_updates)} records in Algolia")
            
            result = {
                "total_ready_for_retry": len(error_records),
                "successful_retries": successful_retries,
                "failed_retries": failed_retries,
                "processing_errors": processing_errors
            }
            
            logger.info(f"Error retry completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error in retry_failed_experts: {str(e)}")
            raise