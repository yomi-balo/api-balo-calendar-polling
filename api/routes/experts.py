import logging
from fastapi import APIRouter, HTTPException, Path, Depends
from tortoise.exceptions import DoesNotExist
from tortoise.transactions import in_transaction
from pydantic import ValidationError

from models.expert import Expert
from schemas.expert import (
    ExpertCalendarList,
    ExpertResponse,
    ExpertListResponse,
    ExpertCreateResponse,
    ExpertUpdate
)
from schemas.availability import AvailabilityData
from schemas.pagination import PaginationParams, PaginatedResponse
from services.expert_service import ExpertService
from services.cronofy_service import CronofyService
from core.expert_utils import delete_expert_by_identifier
from core.cache import cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/experts", tags=["experts"])


@router.post("/calendars", response_model=ExpertCreateResponse)
async def set_expert_calendars(data: ExpertCalendarList):
    """Set or update expert calendar mappings in database"""

    expert_data = [
        {
            "expert_name": expert.expert_name,
            "cronofy_id": expert.cronofy_id,
            "calendar_ids": expert.calendar_ids,
            "bubble_uid": expert.bubble_uid
        }
        for expert in data.experts
    ]

    updated_count = await ExpertService.bulk_upsert_experts(expert_data)

    # Invalidate all cached expert lists after bulk upsert
    await cache.clear()  # Clear all cache since pagination keys are dynamic

    return ExpertCreateResponse(
        message=f"Successfully processed {updated_count} expert calendar mappings",
        updated_count=updated_count
    )


@router.get("/calendars", response_model=PaginatedResponse[dict])
async def get_expert_calendars(pagination: PaginationParams = Depends()):
    """Get expert calendar mappings from database with pagination and caching"""
    cache_key = f"experts_list_p{pagination.page}_l{pagination.limit}"
    
    # Try to get from cache first
    cached_data = await cache.get(cache_key)
    if cached_data:
        logger.debug(f"Returning cached expert list page {pagination.page}")
        return cached_data
    
    # Get total count for pagination
    total_count = await Expert.all().count()
    
    # Fetch paginated data from database
    experts = await Expert.all().order_by('-updated_at').offset(pagination.offset).limit(pagination.limit)
    
    # Format expert data
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
    
    response = PaginatedResponse.create(expert_data, total_count, pagination)
    
    # Cache for 1 minute (paginated data changes more frequently)
    await cache.set(cache_key, response, ttl=60)
    
    return response


@router.get("/{bubble_uid}", response_model=ExpertResponse)
async def get_expert_by_bubble_uid(
    bubble_uid: str = Path(..., min_length=1, max_length=255, description="Expert's Bubble UID")
):
    """Get specific expert details by Bubble UID"""
    expert = await Expert.get_by_bubble_uid(bubble_uid)
    if not expert:
        raise HTTPException(status_code=404, detail="Expert not found")

    return ExpertResponse(
        expert_name=expert.expert_name,
        cronofy_id=expert.cronofy_id,
        calendar_ids=expert.calendar_ids,
        bubble_uid=expert.bubble_uid,
        created_at=expert.created_at,
        updated_at=expert.updated_at,
        last_availability_check=expert.last_availability_check,
        earliest_available_unix=expert.earliest_available_unix
    )


@router.put("/{bubble_uid}", response_model=ExpertResponse)
async def update_expert_by_bubble_uid(
    bubble_uid: str = Path(..., min_length=1, max_length=255, description="Expert's Bubble UID"),
    update_data: ExpertUpdate = ...
):
    """Update expert's cronofy_id and calendar_ids by Bubble UID"""
    try:
        async with in_transaction():
            expert = await Expert.get_by_bubble_uid(bubble_uid)
            if not expert:
                raise HTTPException(status_code=404, detail="Expert not found")

            # Update the expert's fields with version increment
            expert.cronofy_id = update_data.cronofy_id
            expert.calendar_ids = update_data.calendar_ids
            expert.version += 1
            await expert.save(update_fields=['cronofy_id', 'calendar_ids', 'updated_at', 'version'])

            logger.info(
                f"Updated expert {expert.expert_name} (bubble_uid: {bubble_uid}) - cronofy_id: {update_data.cronofy_id}, calendars: {len(update_data.calendar_ids)}")

            # Invalidate cache after update
            await cache.clear()  # Clear all cache since pagination keys are dynamic

            return ExpertResponse(
                expert_name=expert.expert_name,
                cronofy_id=expert.cronofy_id,
                calendar_ids=expert.calendar_ids,
                bubble_uid=expert.bubble_uid,
                created_at=expert.created_at,
                updated_at=expert.updated_at,
                last_availability_check=expert.last_availability_check,
                earliest_available_unix=expert.earliest_available_unix
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update expert {bubble_uid}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error during update")


@router.get("/cronofy/{cronofy_id}", response_model=ExpertResponse)
async def get_expert_by_cronofy_id(
    cronofy_id: str = Path(..., min_length=1, max_length=255, description="Expert's Cronofy ID")
):
    """Get specific expert details by Cronofy ID"""
    expert = await Expert.get_by_cronofy_id(cronofy_id)
    if not expert:
        raise HTTPException(status_code=404, detail="Expert not found")

    return ExpertResponse(
        expert_name=expert.expert_name,
        cronofy_id=expert.cronofy_id,
        calendar_ids=expert.calendar_ids,
        bubble_uid=expert.bubble_uid,
        created_at=expert.created_at,
        updated_at=expert.updated_at,
        last_availability_check=expert.last_availability_check,
        earliest_available_unix=expert.earliest_available_unix
    )


@router.get("/{bubble_uid}/availability", response_model=AvailabilityData)
async def get_expert_availability_by_bubble_uid(
    bubble_uid: str = Path(..., min_length=1, max_length=255, description="Expert's Bubble UID")
):
    """Get cached availability for a specific expert by Bubble UID or fetch fresh data"""
    expert = await Expert.get_by_bubble_uid(bubble_uid)
    if not expert:
        raise HTTPException(status_code=404, detail="Expert not found")

    try:
        availability = await CronofyService.fetch_expert_availability(
            expert.cronofy_id, expert.calendar_ids
        )

        # Update database with fresh data
        await expert.update_availability(availability.earliest_available_unix)

        return availability
    except Exception as e:
        logger.error(f"Error fetching availability for expert {expert.expert_name} ({bubble_uid}): {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching availability")


@router.get("/cronofy/{cronofy_id}/availability", response_model=AvailabilityData)
async def get_expert_availability_by_cronofy_id(
    cronofy_id: str = Path(..., min_length=1, max_length=255, description="Expert's Cronofy ID")
):
    """Get cached availability for a specific expert by Cronofy ID or fetch fresh data"""
    expert = await Expert.get_by_cronofy_id(cronofy_id)
    if not expert:
        raise HTTPException(status_code=404, detail="Expert not found")

    try:
        availability = await CronofyService.fetch_expert_availability(
            expert.cronofy_id, expert.calendar_ids
        )

        # Update database with fresh data
        await expert.update_availability(availability.earliest_available_unix)

        return availability
    except Exception as e:
        logger.error(f"Error fetching availability for expert {expert.expert_name} ({cronofy_id}): {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching availability")


@router.delete("/{bubble_uid}")
async def delete_expert_by_bubble_uid(
    bubble_uid: str = Path(..., min_length=1, max_length=255, description="Expert's Bubble UID")
):
    """Delete an expert from the database by Bubble UID"""
    return await delete_expert_by_identifier(bubble_uid, "bubble_uid", "bubble_uid")


@router.delete("/cronofy/{cronofy_id}")
async def delete_expert_by_cronofy_id(
    cronofy_id: str = Path(..., min_length=1, max_length=255, description="Expert's Cronofy ID")
):
    """Delete an expert from the database by Cronofy ID"""
    return await delete_expert_by_identifier(cronofy_id, "cronofy_id", "cronofy_id")