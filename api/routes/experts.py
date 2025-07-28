import logging
from fastapi import APIRouter, HTTPException
from tortoise.exceptions import DoesNotExist

from models.expert import Expert
from schemas.expert import (
    ExpertCalendarList,
    ExpertResponse,
    ExpertListResponse,
    ExpertCreateResponse
)
from schemas.availability import AvailabilityData
from services.expert_service import ExpertService
from services.cronofy_service import CronofyService

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

    return ExpertCreateResponse(
        message=f"Successfully processed {updated_count} expert calendar mappings",
        updated_count=updated_count
    )


@router.get("/calendars", response_model=ExpertListResponse)
async def get_expert_calendars():
    """Get all expert calendar mappings from database"""
    expert_data = await ExpertService.get_all_experts_with_data()

    return ExpertListResponse(
        experts=expert_data,
        total_count=len(expert_data)
    )


@router.get("/bubble/{bubble_uid}", response_model=ExpertResponse)
async def get_expert_by_bubble_uid(bubble_uid: str):
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


@router.get("/cronofy/{cronofy_id}", response_model=ExpertResponse)
async def get_expert_by_cronofy_id(cronofy_id: str):
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


@router.get("/bubble/{bubble_uid}/availability", response_model=AvailabilityData)
async def get_expert_availability_by_bubble_uid(bubble_uid: str):
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
async def get_expert_availability_by_cronofy_id(cronofy_id: str):
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


@router.delete("/bubble/{bubble_uid}")
async def delete_expert_by_bubble_uid(bubble_uid: str):
    """Delete an expert from the database by Bubble UID"""
    expert = await Expert.get_by_bubble_uid(bubble_uid)
    if not expert:
        raise HTTPException(status_code=404, detail="Expert not found")

    expert_name = expert.expert_name
    await expert.delete()
    logger.info(f"Deleted expert {expert_name} (bubble_uid: {bubble_uid}) from database")
    return {"message": f"Expert {expert_name} deleted successfully"}


@router.delete("/cronofy/{cronofy_id}")
async def delete_expert_by_cronofy_id(cronofy_id: str):
    """Delete an expert from the database by Cronofy ID"""
    expert = await Expert.get_by_cronofy_id(cronofy_id)
    if not expert:
        raise HTTPException(status_code=404, detail="Expert not found")

    expert_name = expert.expert_name
    await expert.delete()
    logger.info(f"Deleted expert {expert_name} (cronofy_id: {cronofy_id}) from database")
    return {"message": f"Expert {expert_name} deleted successfully"}