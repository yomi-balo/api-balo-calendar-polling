import logging
from fastapi import APIRouter, HTTPException, Path, Depends
from tortoise.exceptions import DoesNotExist
from tortoise.transactions import in_transaction
from pydantic import ValidationError

from models.expert import Expert
from models.availability_error import AvailabilityError
from schemas.expert import (
    ExpertCalendarList,
    ExpertResponse,
    ExpertListResponse,
    ExpertCreateResponse,
    ExpertUpdate
)
from schemas.availability import AvailabilityData
from schemas.availability_error import AvailabilityErrorResponse, AvailabilityErrorListResponse
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

        # Check if there was an error in the availability response
        if availability and hasattr(availability, 'error') and availability.error:
            # Log error to availability_errors table
            try:
                await AvailabilityError.log_error(
                    bubble_uid=expert.bubble_uid,
                    expert_name=expert.expert_name,
                    cronofy_id=expert.cronofy_id,
                    error_reason=availability.error,
                    error_details=availability.error_details
                )
            except Exception as log_error:
                logger.error(f"Failed to log availability error for {expert.bubble_uid}: {log_error}")
        else:
            # Success - clear any existing error and update database
            try:
                await AvailabilityError.clear_error(expert.bubble_uid)
            except Exception as clear_error:
                logger.error(f"Failed to clear availability error for {expert.bubble_uid}: {clear_error}")
            
            if availability:
                await expert.update_availability(availability.earliest_available_unix)

        return availability
    except Exception as e:
        logger.error(f"Error fetching availability for expert {expert.expert_name} ({bubble_uid}): {str(e)}")
        
        # Log processing error to availability_errors table
        try:
            await AvailabilityError.log_error(
                bubble_uid=expert.bubble_uid,
                expert_name=expert.expert_name,
                cronofy_id=expert.cronofy_id,
                error_reason="Processing error",
                error_details=f"{type(e).__name__}: {str(e)}"
            )
        except Exception as log_error:
            logger.error(f"Failed to log processing error for {expert.bubble_uid}: {log_error}")
        
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

        # Check if there was an error in the availability response
        if availability and hasattr(availability, 'error') and availability.error:
            # Log error to availability_errors table
            try:
                await AvailabilityError.log_error(
                    bubble_uid=expert.bubble_uid,
                    expert_name=expert.expert_name,
                    cronofy_id=expert.cronofy_id,
                    error_reason=availability.error,
                    error_details=availability.error_details
                )
            except Exception as log_error:
                logger.error(f"Failed to log availability error for {expert.bubble_uid}: {log_error}")
        else:
            # Success - clear any existing error and update database
            try:
                await AvailabilityError.clear_error(expert.bubble_uid)
            except Exception as clear_error:
                logger.error(f"Failed to clear availability error for {expert.bubble_uid}: {clear_error}")
            
            if availability:
                await expert.update_availability(availability.earliest_available_unix)

        return availability
    except Exception as e:
        logger.error(f"Error fetching availability for expert {expert.expert_name} ({cronofy_id}): {str(e)}")
        
        # Log processing error to availability_errors table
        try:
            await AvailabilityError.log_error(
                bubble_uid=expert.bubble_uid,
                expert_name=expert.expert_name,
                cronofy_id=expert.cronofy_id,
                error_reason="Processing error",
                error_details=f"{type(e).__name__}: {str(e)}"
            )
        except Exception as log_error:
            logger.error(f"Failed to log processing error for {expert.bubble_uid}: {log_error}")
        
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


@router.post("/refresh-availability")
async def refresh_all_availability():
    """Manual trigger to refresh availability for all experts (for debugging)"""
    try:
        logger.info("Manual availability refresh triggered")
        await ExpertService.update_all_expert_availability()
        return {"message": "Availability refresh completed successfully"}
    except Exception as e:
        logger.error(f"Manual availability refresh failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Availability refresh failed: {str(e)}")


@router.post("/refresh-availability/{bubble_uid}")
async def refresh_single_expert_availability(
    bubble_uid: str = Path(..., min_length=1, max_length=255, description="Expert's Bubble UID")
):
    """Manual trigger to refresh availability for a single expert (for debugging)"""
    try:
        expert = await Expert.get_by_bubble_uid(bubble_uid)
        if not expert:
            raise HTTPException(status_code=404, detail="Expert not found")
        
        logger.info(f"Manual availability refresh triggered for expert {expert.expert_name}")
        
        # Fetch fresh availability data
        availability = await CronofyService.fetch_expert_availability(
            expert.cronofy_id, expert.calendar_ids
        )
        
        # Check if there was an error in the availability response
        if availability and hasattr(availability, 'error') and availability.error:
            # Log error to availability_errors table
            try:
                await AvailabilityError.log_error(
                    bubble_uid=expert.bubble_uid,
                    expert_name=expert.expert_name,
                    cronofy_id=expert.cronofy_id,
                    error_reason=availability.error,
                    error_details=availability.error_details
                )
            except Exception as log_error:
                logger.error(f"Failed to log availability error for {expert.bubble_uid}: {log_error}")
            
            return {
                "message": f"Availability refresh failed for {expert.expert_name}",
                "expert_name": expert.expert_name,
                "error": availability.error,
                "error_details": availability.error_details
            }
        else:
            # Success - clear any existing error and update database
            try:
                await AvailabilityError.clear_error(expert.bubble_uid)
            except Exception as clear_error:
                logger.error(f"Failed to clear availability error for {expert.bubble_uid}: {clear_error}")
            
            old_timestamp = expert.earliest_available_unix
            await expert.update_availability(availability.earliest_available_unix)
            
            return {
                "message": f"Availability refresh completed for {expert.expert_name}",
                "expert_name": expert.expert_name,
                "old_timestamp": old_timestamp,
                "new_timestamp": availability.earliest_available_unix,
                "timestamp_changed": old_timestamp != availability.earliest_available_unix
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Manual availability refresh failed for {bubble_uid}: {str(e)}")
        
        # Log processing error to availability_errors table if we have the expert
        if 'expert' in locals():
            await AvailabilityError.log_error(
                bubble_uid=expert.bubble_uid,
                expert_name=expert.expert_name,
                cronofy_id=expert.cronofy_id,
                error_reason="Processing error",
                error_details=f"{type(e).__name__}: {str(e)}"
            )
        
        raise HTTPException(status_code=500, detail=f"Availability refresh failed: {str(e)}")


@router.get("/availability-errors", response_model=AvailabilityErrorListResponse)
async def get_availability_errors():
    """Get all current availability errors (experts that are currently failing)"""
    try:
        error_records = await AvailabilityError.get_all_errors()
        
        errors = [
            AvailabilityErrorResponse(
                bubble_uid=error.bubble_uid,
                expert_name=error.expert_name,
                cronofy_id=error.cronofy_id,
                error_reason=error.error_reason,
                error_details=error.error_details,
                unix_timestamp=error.unix_timestamp,
                melbourne_time=error.melbourne_time,
                created_at=error.created_at,
                updated_at=error.updated_at
            )
            for error in error_records
        ]
        
        return AvailabilityErrorListResponse(
            errors=errors,
            total_count=len(errors),
            message=f"Found {len(errors)} experts currently experiencing availability check failures"
        )
        
    except Exception as e:
        logger.error(f"Error retrieving availability errors: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving availability errors")


@router.get("/availability-errors/{bubble_uid}", response_model=AvailabilityErrorResponse)
async def get_availability_error_by_bubble_uid(
    bubble_uid: str = Path(..., min_length=1, max_length=255, description="Expert's Bubble UID")
):
    """Get availability error for a specific expert by Bubble UID"""
    try:
        error_record = await AvailabilityError.get_error_by_bubble_uid(bubble_uid)
        
        if not error_record:
            raise HTTPException(
                status_code=404, 
                detail="No availability error found for this expert (expert may be working correctly)"
            )
        
        return AvailabilityErrorResponse(
            bubble_uid=error_record.bubble_uid,
            expert_name=error_record.expert_name,
            cronofy_id=error_record.cronofy_id,
            error_reason=error_record.error_reason,
            error_details=error_record.error_details,
            unix_timestamp=error_record.unix_timestamp,
            melbourne_time=error_record.melbourne_time,
            created_at=error_record.created_at,
            updated_at=error_record.updated_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving availability error for {bubble_uid}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving availability error")