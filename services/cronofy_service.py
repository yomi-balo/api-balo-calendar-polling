import asyncio
import httpx
import logging
from datetime import datetime, timezone
from typing import List, Optional

from config.settings import settings
from models.expert import Expert
from schemas.availability import AvailabilityData

logger = logging.getLogger(__name__)


class CronofyService:
    """Service for handling Cronofy API interactions"""

    @staticmethod
    def find_earliest_available_slot(cronofy_data: dict) -> Optional[int]:
        """Process Cronofy free/busy data to find the earliest available time slot"""
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

    @staticmethod
    async def fetch_experts_availability_batch(experts: List[Expert]) -> List[AvailabilityData]:
        """Fetch availability data from Cronofy for multiple experts in one request"""

        # Calculate total calendar IDs across all experts
        total_calendar_ids = sum(len(expert.calendar_ids) for expert in experts)

        if total_calendar_ids > settings.CRONOFY_MAX_CALENDARS_PER_REQUEST:
            raise ValueError(
                f"Total calendar IDs ({total_calendar_ids}) exceeds Cronofy's limit of {settings.CRONOFY_MAX_CALENDARS_PER_REQUEST} per request")

        if not settings.CRONOFY_ACCESS_TOKEN:
            logger.warning("CRONOFY_ACCESS_TOKEN not set - returning empty availability")
            return [
                AvailabilityData(
                    expert_id=expert.cronofy_id,  # Use cronofy_id as identifier
                    earliest_available_unix=None,
                    last_updated=datetime.now(timezone.utc).isoformat()
                )
                for expert in experts
            ]

        headers = {
            "Authorization": f"Bearer {settings.CRONOFY_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }

        from_time = datetime.now(timezone.utc).isoformat()
        to_time = (datetime.now(timezone.utc).replace(
            day=min(31, datetime.now().day + 30),
            hour=23, minute=59, second=59, microsecond=0
        )).isoformat()

        # Build calendar_ids array and mapping
        all_calendar_ids = []
        expert_calendar_mapping = {}

        for expert in experts:
            for calendar_id in expert.calendar_ids:
                all_calendar_ids.append(calendar_id)
                expert_calendar_mapping[calendar_id] = expert.cronofy_id

        params = {
            "from": from_time,
            "to": to_time
        }

        for calendar_id in all_calendar_ids:
            params[f"calendar_ids[]"] = calendar_id

        async with httpx.AsyncClient(timeout=settings.CRONOFY_REQUEST_TIMEOUT) as client:
            response = await client.get(settings.CRONOFY_API_URL, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()

            # Process response
            results = []
            free_busy_data = data.get("free_busy", [])

            expert_availability = {}
            for item in free_busy_data:
                calendar_id = item.get("calendar_id")
                if calendar_id in expert_calendar_mapping:
                    cronofy_id = expert_calendar_mapping[calendar_id]
                    if cronofy_id not in expert_availability:
                        expert_availability[cronofy_id] = []
                    expert_availability[cronofy_id].append(item)

            for expert in experts:
                expert_free_busy = expert_availability.get(expert.cronofy_id, [])
                earliest_available = CronofyService.find_earliest_available_slot({"free_busy": expert_free_busy})

                results.append(AvailabilityData(
                    expert_id=expert.cronofy_id,  # Use cronofy_id as identifier
                    earliest_available_unix=earliest_available,
                    last_updated=datetime.now(timezone.utc).isoformat()
                ))

            return results

    @staticmethod
    async def fetch_expert_availability(cronofy_id: str, calendar_ids: List[str]) -> AvailabilityData:
        """Fetch availability data from Cronofy for a single expert with multiple calendars"""

        # Create dummy expert for batch processing
        class DummyExpert:
            def __init__(self, cronofy_id, calendar_ids):
                self.cronofy_id = cronofy_id
                self.calendar_ids = calendar_ids

        dummy_expert = DummyExpert(cronofy_id, calendar_ids)
        batch_results = await CronofyService.fetch_experts_availability_batch([dummy_expert])
        return batch_results[0]