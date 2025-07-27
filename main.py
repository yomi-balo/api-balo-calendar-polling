from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import asyncio
import httpx
import logging
from datetime import datetime, timezone
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from algoliasearch.search_client import SearchClient
from contextlib import asynccontextmanager
from tortoise.models import Model
from tortoise import fields, Tortoise
from tortoise.exceptions import DoesNotExist

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
CRONOFY_ACCESS_TOKEN = os.getenv("CRONOFY_ACCESS_TOKEN")
ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_API_KEY = os.getenv("ALGOLIA_API_KEY")
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME", "experts")
DATABASE_URL = os.getenv("DATABASE_URL", "postgres://user:password@localhost:5432/calendar_db")

# Initialize Algolia client (with error handling)
algolia_client = None
algolia_index = None

def init_algolia():
    global algolia_client, algolia_index
    if ALGOLIA_APP_ID and ALGOLIA_API_KEY:
        try:
            algolia_client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_API_KEY)
            algolia_index = algolia_client.init_index(ALGOLIA_INDEX_NAME)
            logger.info("Algolia client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Algolia: {e}")
    else:
        logger.warning("Algolia credentials not provided - Algolia features will be disabled")


# Tortoise ORM Models
class Expert(Model):
    id = fields.IntField(pk=True)
    expert_id = fields.CharField(max_length=255, unique=True, index=True)
    cronofy_calendar_ids = fields.JSONField()  # Store list of calendar IDs
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)
    last_availability_check = fields.DatetimeField(null=True)
    earliest_available_unix = fields.BigIntField(null=True)

    class Meta:
        table = "experts"

    def __str__(self):
        return f"Expert({self.expert_id})"


# Pydantic models for API
class ExpertCalendar(BaseModel):
    expert_id: str
    cronofy_calendar_ids: List[str]


class ExpertCalendarList(BaseModel):
    experts: List[ExpertCalendar]


class AvailabilityData(BaseModel):
    expert_id: str
    earliest_available_unix: Optional[int]
    last_updated: str


class ExpertResponse(BaseModel):
    expert_id: str
    cronofy_calendar_ids: List[str]
    created_at: datetime
    updated_at: datetime
    last_availability_check: Optional[datetime] = None
    earliest_available_unix: Optional[int] = None


# Database operations using Tortoise ORM
async def upsert_expert(expert_id: str, cronofy_calendar_ids: List[str]) -> Expert:
    """Insert or update expert record"""
    expert, created = await Expert.get_or_create(
        expert_id=expert_id,
        defaults={
            'cronofy_calendar_ids': cronofy_calendar_ids,
        }
    )

    if not created:
        # Update existing expert
        expert.cronofy_calendar_ids = cronofy_calendar_ids
        expert.updated_at = datetime.now(timezone.utc)
        await expert.save(update_fields=['cronofy_calendar_ids', 'updated_at'])

    return expert


async def get_all_experts() -> List[Expert]:
    """Get all experts from database"""
    return await Expert.all().order_by('-updated_at')


async def update_expert_availability(expert_id: str, earliest_available_unix: Optional[int]):
    """Update expert's availability data"""
    await Expert.filter(expert_id=expert_id).update(
        last_availability_check=datetime.now(timezone.utc),
        earliest_available_unix=earliest_available_unix
    )


# App context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup - Initialize Tortoise ORM
    await Tortoise.init(
        db_url=DATABASE_URL,
        modules={'models': ['__main__']}  # Use current module
    )
    await Tortoise.generate_schemas()  # Create tables if they don't exist

    # Initialize Algolia
    init_algolia()

    # Start scheduler
    scheduler.add_job(
        update_all_expert_availability,
        "interval",
        minutes=5,
        id="update_availability",
        replace_existing=True
    )
    scheduler.start()
    logger.info("Calendar caching API started with 5-minute scheduler")

    # Run initial update
    await update_all_expert_availability()

    yield

    # Shutdown
    scheduler.shutdown()
    await Tortoise.close_connections()
    logger.info("Application shutdown complete")


app = FastAPI(
    title="Calendar Caching API",
    version="1.0.0",
    lifespan=lifespan
)


# API Routes
@app.get("/")
async def root():
    return {"message": "Calendar Caching API is running"}


@app.post("/experts/calendars")
async def set_expert_calendars(data: ExpertCalendarList):
    """Set or update expert calendar mappings in database"""

    updated_count = 0
    for expert in data.experts:
        await upsert_expert(expert.expert_id, expert.cronofy_calendar_ids)
        updated_count += 1

    logger.info(f"Upserted {updated_count} expert calendar mappings to database")

    return {
        "message": f"Successfully processed {updated_count} expert calendar mappings",
        "updated_count": updated_count
    }


@app.get("/experts/calendars", response_model=dict)
async def get_expert_calendars():
    """Get all expert calendar mappings from database"""
    experts = await get_all_experts()

    expert_data = []
    for expert in experts:
        expert_data.append({
            "expert_id": expert.expert_id,
            "cronofy_calendar_ids": expert.cronofy_calendar_ids,
            "created_at": expert.created_at.isoformat(),
            "updated_at": expert.updated_at.isoformat(),
            "last_availability_check": expert.last_availability_check.isoformat() if expert.last_availability_check else None,
            "earliest_available_unix": expert.earliest_available_unix
        })

    return {
        "experts": expert_data,
        "total_count": len(expert_data)
    }


@app.get("/experts/{expert_id}", response_model=ExpertResponse)
async def get_expert(expert_id: str):
    """Get specific expert details"""
    try:
        expert = await Expert.get(expert_id=expert_id)
        return ExpertResponse(
            expert_id=expert.expert_id,
            cronofy_calendar_ids=expert.cronofy_calendar_ids,
            created_at=expert.created_at,
            updated_at=expert.updated_at,
            last_availability_check=expert.last_availability_check,
            earliest_available_unix=expert.earliest_available_unix
        )
    except DoesNotExist:
        raise HTTPException(status_code=404, detail="Expert not found")


@app.get("/experts/{expert_id}/availability")
async def get_expert_availability(expert_id: str):
    """Get cached availability for a specific expert or fetch fresh data"""
    try:
        expert = await Expert.get(expert_id=expert_id)

        availability = await fetch_expert_availability(expert_id, expert.cronofy_calendar_ids)

        # Update database with fresh data
        await update_expert_availability(expert_id, availability.earliest_available_unix)

        return availability
    except DoesNotExist:
        raise HTTPException(status_code=404, detail="Expert not found")
    except Exception as e:
        logger.error(f"Error fetching availability for expert {expert_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching availability")


@app.delete("/experts/{expert_id}")
async def delete_expert(expert_id: str):
    """Delete an expert from the database"""
    try:
        expert = await Expert.get(expert_id=expert_id)
        await expert.delete()
        logger.info(f"Deleted expert {expert_id} from database")
        return {"message": f"Expert {expert_id} deleted successfully"}
    except DoesNotExist:
        raise HTTPException(status_code=404, detail="Expert not found")


# Core Functions (same as before)
async def fetch_experts_availability_batch(experts: List[Expert]) -> List[AvailabilityData]:
    """Fetch availability data from Cronofy for multiple experts in one request"""

    # Calculate total calendar IDs across all experts
    total_calendar_ids = sum(len(expert.cronofy_calendar_ids) for expert in experts)

    if total_calendar_ids > 15:
        raise ValueError(f"Total calendar IDs ({total_calendar_ids}) exceeds Cronofy's limit of 15 per request")

    headers = {
        "Authorization": f"Bearer {CRONOFY_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    url = "https://api.cronofy.com/v1/free_busy"

    from_time = datetime.now(timezone.utc).isoformat()
    to_time = (datetime.now(timezone.utc).replace(
        day=min(31, datetime.now().day + 30),
        hour=23, minute=59, second=59, microsecond=0
    )).isoformat()

    # Build calendar_ids array and mapping
    all_calendar_ids = []
    expert_calendar_mapping = {}

    for expert in experts:
        for calendar_id in expert.cronofy_calendar_ids:
            all_calendar_ids.append(calendar_id)
            expert_calendar_mapping[calendar_id] = expert.expert_id

    params = {
        "from": from_time,
        "to": to_time
    }

    for calendar_id in all_calendar_ids:
        params[f"calendar_ids[]"] = calendar_id

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()

        # Process response
        results = []
        free_busy_data = data.get("free_busy", [])

        expert_availability = {}
        for item in free_busy_data:
            calendar_id = item.get("calendar_id")
            if calendar_id in expert_calendar_mapping:
                expert_id = expert_calendar_mapping[calendar_id]
                if expert_id not in expert_availability:
                    expert_availability[expert_id] = []
                expert_availability[expert_id].append(item)

        for expert in experts:
            expert_free_busy = expert_availability.get(expert.expert_id, [])
            earliest_available = find_earliest_available_slot({"free_busy": expert_free_busy})

            results.append(AvailabilityData(
                expert_id=expert.expert_id,
                earliest_available_unix=earliest_available,
                last_updated=datetime.now(timezone.utc).isoformat()
            ))

        return results


async def fetch_expert_availability(expert_id: str, calendar_ids: List[str]) -> AvailabilityData:
    """Fetch availability data from Cronofy for a single expert with multiple calendars"""

    # Create dummy expert for batch processing
    class DummyExpert:
        def __init__(self, expert_id, calendar_ids):
            self.expert_id = expert_id
            self.cronofy_calendar_ids = calendar_ids

    dummy_expert = DummyExpert(expert_id, calendar_ids)
    batch_results = await fetch_experts_availability_batch([dummy_expert])
    return batch_results[0]


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


async def update_all_expert_availability():
    """Fetch availability for all experts from database in batches and update Algolia"""
    experts = await get_all_experts()

    if not experts:
        logger.info("No experts found in database")
        return

    logger.info(f"Updating availability for {len(experts)} experts from database using smart batching")

    algolia_updates = []

    # Smart batching logic
    expert_batches = []
    current_batch = []
    current_calendar_count = 0

    for expert in experts:
        expert_calendar_count = len(expert.cronofy_calendar_ids)

        if current_calendar_count + expert_calendar_count > 15:
            if current_batch:
                expert_batches.append(current_batch)
            current_batch = [expert]
            current_calendar_count = expert_calendar_count
        else:
            current_batch.append(expert)
            current_calendar_count += expert_calendar_count

    if current_batch:
        expert_batches.append(current_batch)

    total_processed = 0
    total_failed = 0

    for batch_idx, expert_batch in enumerate(expert_batches):
        try:
            batch_calendar_count = sum(len(expert.cronofy_calendar_ids) for expert in expert_batch)
            logger.info(f"Processing batch {batch_idx + 1}/{len(expert_batches)} with {len(expert_batch)} experts "
                        f"and {batch_calendar_count} calendar IDs")

            availability_results = await fetch_experts_availability_batch(expert_batch)

            for expert, availability in zip(expert_batch, availability_results):
                try:
                    await update_expert_availability(
                        expert.expert_id,
                        availability.earliest_available_unix
                    )

                    algolia_record = {
                        "objectID": expert.expert_id,
                        "earliest_available_unix": availability.earliest_available_unix,
                        "availability_last_updated": availability.last_updated
                    }

                    algolia_updates.append(algolia_record)
                    total_processed += 1

                except Exception as e:
                    logger.error(f"Failed to process expert {expert.expert_id}: {str(e)}")
                    total_failed += 1

            if batch_idx < len(expert_batches) - 1:
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Failed to process batch {batch_idx + 1}: {str(e)}")
            total_failed += len(expert_batch)

    # Update Algolia
    # Update Algolia
    if algolia_updates and algolia_index:  # Check if algolia_index exists
        try:
            algolia_batch_size = 100
            algolia_batches = [algolia_updates[i:i + algolia_batch_size]
                               for i in range(0, len(algolia_updates), algolia_batch_size)]

            for algolia_batch in algolia_batches:
                algolia_index.partial_update_objects(algolia_batch)

            logger.info(f"Successfully updated {len(algolia_updates)} expert records in Algolia")
        except Exception as e:
            logger.error(f"Failed to update Algolia: {str(e)}")
    elif algolia_updates and not algolia_index:
        logger.warning("Algolia updates skipped - Algolia not configured")

    logger.info(f"Processing complete. Processed: {total_processed}, Failed: {total_failed}")


# Scheduler setup
scheduler = AsyncIOScheduler()


# Health check endpoint
@app.get("/health")
async def health_check():
    try:
        expert_count = await Expert.all().count()

        return {
            "status": "healthy",
            "experts_in_database": expert_count,
            "scheduler_running": scheduler.running if scheduler else False,
            "database_connected": True,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "database_connected": False,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }