import asyncio
import httpx
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
import time

from config.settings import settings
from models.expert import Expert
from schemas.availability import AvailabilityData
from core.retry_utils import with_retry
from core.logging_utils import get_structured_logger

logger = logging.getLogger(__name__)
structured_logger = get_structured_logger(__name__)


class CronofyService:
    """Service for handling Cronofy API interactions"""

    CRONOFY_API_BASE = "https://api-au.cronofy.com/v1"
    _client: Optional[httpx.AsyncClient] = None
    _last_request_time: float = 0
    _min_request_interval: float = 0.5  # 500ms between requests

    @classmethod
    async def get_client(cls) -> httpx.AsyncClient:
        """Get or create shared HTTP client with connection pooling"""
        if cls._client is None:
            cls._client = httpx.AsyncClient(
                timeout=25.0,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
            )
        return cls._client

    @classmethod
    async def close_client(cls):
        """Close shared HTTP client"""
        if cls._client is not None:
            await cls._client.aclose()
            cls._client = None

    @classmethod
    async def _rate_limit(cls):
        """Implement rate limiting for API calls"""
        current_time = time.time()
        time_since_last = current_time - cls._last_request_time
        
        if time_since_last < cls._min_request_interval:
            wait_time = cls._min_request_interval - time_since_last
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s before next API call")
            await asyncio.sleep(wait_time)
        
        cls._last_request_time = time.time()

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

        # Format timestamps to match Cronofy's expected format (no microseconds, use Z)
        start_time_str = now.replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time_str = end_time.replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')

        periods = [
            {
                "start": start_time_str,
                "end": end_time_str
            }
        ]

        logger.info(f"Created query periods: from {start_time_str} to {end_time_str} ({days_ahead} days)")
        return periods

    @staticmethod
    def batch_experts(experts: List[Expert], batch_size: int = 10) -> List[List[Expert]]:
        """Batch experts into groups of specified size (default 10)"""
        batches = []
        for i in range(0, len(experts), batch_size):
            batches.append(experts[i:i + batch_size])
        return batches

    @staticmethod
    @with_retry(
        max_retries=3,
        base_delay=1.0,
        max_delay=10.0,
        exceptions=(httpx.TimeoutException, httpx.ConnectTimeout, httpx.HTTPStatusError)
    )
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

        # LOG: Request details (without sensitive data)
        logger.info("=== CRONOFY API REQUEST DEBUG ===")
        logger.info(f"URL: {url}")
        logger.info(f"Method: POST")
        logger.info(f"Request Body: {request_body}")
        logger.info("================================")

        try:
            # Apply rate limiting before making the request
            await CronofyService._rate_limit()
            
            client = await CronofyService.get_client()
            response = await client.post(url, headers=headers, json=request_body)

            # LOG: Response details
            logger.info("=== CRONOFY API RESPONSE DEBUG ===")
            logger.info(f"Status Code: {response.status_code}")
            logger.info(f"Status Text: {response.reason_phrase}")
            logger.info(f"Response Headers: {dict(response.headers)}")
            logger.info(f"Response Body: {response.text}")
            logger.info("==================================")

            # If 401 on availability endpoint, log error without token
            if response.status_code == 401:
                logger.error("=== 401 UNAUTHORIZED DEBUG ===")
                logger.error(f"URL attempted: {url}")
                logger.error(f"Token configured: {bool(settings.CRONOFY_ACCESS_TOKEN)}")
                logger.error(
                    f"Token length: {len(settings.CRONOFY_ACCESS_TOKEN) if settings.CRONOFY_ACCESS_TOKEN else 0}")
                logger.error("Common causes:")
                logger.error("1. Invalid or expired token")
                logger.error("2. Token doesn't have availability permissions")
                logger.error("3. Wrong API region (try global vs Australian endpoint)")
                logger.error("4. Token format issue (should start with 'app_' or similar)")
                logger.error("==============================")

            if not response.is_success:
                try:
                    error_data = response.json()
                except:
                    error_data = {"raw_text": response.text}

                logger.error(f"Cronofy API error: {response.status_code} {response.reason_phrase}, {error_data}")
                
                # Raise specific HTTP error for retry mechanism
                if response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"Server error: {response.status_code} {response.reason_phrase}",
                        request=response.request,
                        response=response
                    )
                else:
                    raise Exception(f"Cronofy API error: {response.status_code} {response.reason_phrase}")

            data = response.json()

            # LOG: Successful response summary
            logger.info(f"SUCCESS: Got {len(data.get('available_slots', []))} available slots from Cronofy")

            # Enrich with UIDs if we have original experts with bubble_uids
            if data.get("available_slots") and original_experts:
                # Create a map of cronofy_id (sub) to bubble_uid for quick lookup
                sub_to_uid_map = {
                    expert.cronofy_id: expert.bubble_uid
                    for expert in original_experts
                    if expert.cronofy_id and expert.bubble_uid
                }

                logger.info(f"Expert mapping: {sub_to_uid_map}")

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
                    logger.info("Enriched response with bubble UIDs")

            return data

        except httpx.TimeoutException:
            logger.error("Cronofy API request timed out after 25 seconds")
            raise Exception("Cronofy API request timed out")
        except Exception as e:
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

        if len(experts) > 10:  # Match your JS batch size
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
            # LOG: Input parameters with structured logging
            structured_logger.info(
                "Starting batch availability request",
                batch_size=len(experts),
                duration_minutes=duration,
                buffer_before_minutes=buffer_before,
                buffer_after_minutes=buffer_after,
                days_ahead=days_ahead,
                expert_cronofy_ids=[expert.cronofy_id for expert in experts],
                expert_bubble_uids=[expert.bubble_uid for expert in experts]
            )

            # Create query periods
            query_periods = CronofyService.create_default_query_periods(days_ahead)
            logger.info(f"Query periods: {query_periods}")

            # Create request body
            request_body = CronofyService.create_availability_request_body(
                experts, query_periods, duration, buffer_before, buffer_after
            )

            logger.info("=== REQUEST BODY STRUCTURE ===")
            logger.info(f"Participants count: {len(request_body.get('participants', []))}")
            if request_body.get('participants'):
                members = request_body['participants'][0].get('members', [])
                logger.info(f"Members in first participant group: {len(members)}")
                for i, member in enumerate(members):
                    logger.info(
                        f"  Member {i + 1}: sub={member.get('sub')}, calendar_ids={member.get('calendar_ids')}, managed_availability={member.get('managed_availability')}")

            logger.info(f"Required duration: {request_body.get('required_duration')}")
            logger.info(f"Buffer settings: {request_body.get('buffer')}")
            logger.info(f"Max results: {request_body.get('max_results')}")
            logger.info(f"Response format: {request_body.get('response_format')}")
            logger.info("==============================")

            # Fetch availability
            response_data = await CronofyService.fetch_cronofy_availability(request_body, experts)

            # Process results for each expert
            results = []
            for expert in experts:
                earliest_available = CronofyService.find_earliest_available_slot_from_response(
                    response_data, expert.cronofy_id
                )

                logger.info(f"Expert {expert.cronofy_id} earliest available: {earliest_available}")

                results.append(AvailabilityData(
                    expert_id=expert.cronofy_id,
                    earliest_available_unix=earliest_available,
                    last_updated=datetime.now(timezone.utc).isoformat()
                ))

            return results

        except Exception as e:
            structured_logger.error(
                "Error in batch availability fetch",
                error=str(e),
                error_type=type(e).__name__,
                expert_count=len(experts),
                expert_cronofy_ids=[expert.cronofy_id for expert in experts]
            )
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