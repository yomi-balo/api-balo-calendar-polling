import asyncio
import httpx
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any

from config.settings import settings
from models.expert import Expert
from schemas.availability import AvailabilityData

logger = logging.getLogger(__name__)


class CronofyService:
    """Service for handling Cronofy API interactions"""

    # Use Australian API endpoint to match your production setup
    CRONOFY_API_BASE = "https://api-au.cronofy.com/v1"

    @staticmethod
    def create_availability_request_body(
            member_batch: List[Expert],
            query_periods: List[Dict],
            duration: int = 60,
            buffer_before: int = 0,
            buffer_after: int = 0
    ) -> Dict[str, Any]:
        """Create availability request body"""
        return {
            "participants": [
                {
                    "members": [
                        {
                            "sub": expert.cronofy_id,
                            "calendar_ids": expert.calendar_ids,
                            "managed_availability": True,
                        }
                        for expert in member_batch
                    ],
                    "required": 1,
                }
            ],
            "query_periods": query_periods,
            "required_duration": {
                "minutes": duration,
            },
            "buffer": {
                "before": {"minutes": buffer_before},
                "after": {"minutes": buffer_after},
            },
            "max_results": 512,
            "response_format": "slots",
        }

    @staticmethod
    def create_default_query_periods(days_ahead: int = 30) -> List[Dict]:
        """Create default query periods for the next N days"""
        now = datetime.now(timezone.utc)
        end_time = now + timedelta(days=days_ahead)

        return [
            {
                "start": now.isoformat(),
                "end": end_time.isoformat()
            }
        ]

    @staticmethod
    def batch_experts(experts: List[Expert], batch_size: int = 15) -> List[List[Expert]]:
        """Batch experts into groups of specified size"""
        batches = []
        for i in range(0, len(experts), batch_size):
            batches.append(experts[i:i + batch_size])
        return batches

    @staticmethod
    async def fetch_cronofy_availability(
            request_body: Dict[str, Any],
            original_experts: List[Expert] = None
    ) -> Dict[str, Any]:
        """Fetch availability from Cronofy API"""

        if not settings.CRONOFY_ACCESS_TOKEN:
            raise ValueError("CRONOFY_ACCESS_TOKEN is not configured")

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.CRONOFY_ACCESS_TOKEN}",
        }

        url = f"{CronofyService.CRONOFY_API_BASE}/availability"

        logger.info(f"Cronofy Request Body 1: {request_body}")
        logger.info(f"Cronofy Request Header 1: {headers}")
        try:
            # 25 second timeout
            async with httpx.AsyncClient(timeout=25.0) as client:
                response = await client.post(url, headers=headers, json=request_body)

                logger.error(f"Cronofy Request Body 2: {request_body}")
                logger.error(f"Cronofy Request Header 2: {headers}")

                if not response.is_success:
                    try:
                        error_data = response.json()
                    except:
                        error_data = {}
                    logger.error(f"Cronofy API error: {response.status_code} {response.reason_phrase}, {error_data}: {request_body} || {headers}")
                    raise Exception(f"Cronofy API error: {response.status_code} {response.reason_phrase}")

                data = response.json()

                # Enrich with UIDs if we have original experts with bubble_uids
                if data.get("available_slots") and original_experts:
                    # Create a map of cronofy_id (sub) to bubble_uid for quick lookup
                    sub_to_uid_map = {
                        expert.cronofy_id: expert.bubble_uid
                        for expert in original_experts
                        if expert.cronofy_id and expert.bubble_uid
                    }

                    # Only enrich with UIDs if we have mapping data
                    if sub_to_uid_map:
                        data["available_slots"] = [
                            {
                                **slot,
                                "participants": [
                                    {
                                        **participant,
                                        "uid": sub_to_uid_map.get(participant.get("sub"))
                                    }
                                    for participant in slot.get("participants", [])
                                ]
                            }
                            for slot in data["available_slots"]
                        ]

                return data

        except httpx.TimeoutException:
            logger.error("Cronofy API request timed out after 25 seconds")
            raise Exception("Cronofy API request timed out")
        except Exception as e:
            logger.debug(f"Cronofy Request Body: {request_body}")
            logger.debug(f"Cronofy Request Header: {headers}")
            logger.error(f"Error fetching availability from Cronofy: {str(e)}")
            raise

    @staticmethod
    def find_earliest_available_slot_from_response(cronofy_response: Dict[str, Any], expert_cronofy_id: str) -> \
    Optional[int]:
        """Find earliest available slot for a specific expert from Cronofy availability response"""
        try:
            available_slots = cronofy_response.get("available_slots", [])

            if not available_slots:
                return None

            # Find slots where this expert is a participant
            expert_slots = []
            for slot in available_slots:
                for participant in slot.get("participants", []):
                    if participant.get("sub") == expert_cronofy_id:
                        expert_slots.append(slot)
                        break

            if not expert_slots:
                return None

            # Sort by start time and return earliest
            expert_slots.sort(key=lambda x: x.get("start", ""))
            earliest_slot = expert_slots[0]

            start_time = earliest_slot.get("start")
            if start_time:
                dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                return int(dt.timestamp())

            return None

        except Exception as e:
            logger.error(f"Error processing availability response: {str(e)}")
            return None

    @staticmethod
    async def fetch_experts_availability_batch(
            experts: List[Expert],
            duration: int = 60,
            buffer_before: int = 0,
            buffer_after: int = 0,
            days_ahead: int = 30
    ) -> List[AvailabilityData]:
        """Fetch availability data from Cronofy for multiple experts using new API"""

        if len(experts) > 15:
            raise ValueError(f"Cannot batch more than 10 experts per request (got {len(experts)})")

        if not settings.CRONOFY_ACCESS_TOKEN:
            logger.warning("CRONOFY_ACCESS_TOKEN not set - returning empty availability")
            return [
                AvailabilityData(
                    expert_id=expert.cronofy_id,
                    earliest_available_unix=None,
                    last_updated=datetime.now(timezone.utc).isoformat()
                )
                for expert in experts
            ]

        try:
            # Create query periods
            query_periods = CronofyService.create_default_query_periods(days_ahead)

            # Create request body
            request_body = CronofyService.create_availability_request_body(
                experts, query_periods, duration, buffer_before, buffer_after
            )

            # Fetch availability
            response_data = await CronofyService.fetch_cronofy_availability(request_body, experts)

            # Process results for each expert
            results = []
            for expert in experts:
                earliest_available = CronofyService.find_earliest_available_slot_from_response(
                    response_data, expert.cronofy_id
                )

                results.append(AvailabilityData(
                    expert_id=expert.cronofy_id,
                    earliest_available_unix=earliest_available,
                    last_updated=datetime.now(timezone.utc).isoformat()
                ))

            return results

        except Exception as e:
            logger.error(f"Error in batch availability fetch: {str(e)}")
            # Return empty availability for all experts on error
            return [
                AvailabilityData(
                    expert_id=expert.cronofy_id,
                    earliest_available_unix=None,
                    last_updated=datetime.now(timezone.utc).isoformat()
                )
                for expert in experts
            ]

    @staticmethod
    async def fetch_expert_availability(
            cronofy_id: str,
            calendar_ids: List[str],
            duration: int = 60,
            buffer_before: int = 0,
            buffer_after: int = 0
    ) -> AvailabilityData:
        """Fetch availability data from Cronofy for a single expert"""

        # Create dummy expert for batch processing
        class DummyExpert:
            def __init__(self, cronofy_id, calendar_ids):
                self.cronofy_id = cronofy_id
                self.calendar_ids = calendar_ids
                self.bubble_uid = f"temp_{cronofy_id}"

        dummy_expert = DummyExpert(cronofy_id, calendar_ids)
        batch_results = await CronofyService.fetch_experts_availability_batch(
            [dummy_expert], duration, buffer_before, buffer_after
        )
        return batch_results[0]

    # Keep the old method for backward compatibility but mark as deprecated
    @staticmethod
    def find_earliest_available_slot(cronofy_data: dict) -> Optional[int]:
        """
        DEPRECATED: Process Cronofy free/busy data to find the earliest available time slot
        Use find_earliest_available_slot_from_response instead
        """
        logger.warning("find_earliest_available_slot is deprecated, use find_earliest_available_slot_from_response")
        try:
            free_busy = cronofy_data.get("free_busy", [])

            if not free_busy:
                return None

            for slot in free_busy:
                if slot.get("free_busy_status") == "free":
                    start_time = slot.get("start")
                    if start_time:
                        dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        return int(dt.timestamp())

            return None

        except Exception as e:
            logger.error(f"Error processing availability data: {str(e)}")
            return None