"""Shared utilities for expert operations"""

import logging
from fastapi import HTTPException
from tortoise.transactions import in_transaction
from models.expert import Expert
from core.cache import cache

logger = logging.getLogger(__name__)


async def delete_expert_by_identifier(identifier: str, lookup_field: str, identifier_name: str) -> dict:
    """Generic delete expert function that works with different identifier fields"""
    try:
        async with in_transaction():
            if lookup_field == "bubble_uid":
                expert = await Expert.get_by_bubble_uid(identifier)
            elif lookup_field == "cronofy_id":
                expert = await Expert.get_by_cronofy_id(identifier)
            else:
                raise ValueError(f"Unsupported lookup field: {lookup_field}")
            
            if not expert:
                raise HTTPException(status_code=404, detail="Expert not found")

            expert_name = expert.expert_name
            await expert.delete()
            
            # Invalidate cache after deletion
            await cache.clear()  # Clear all cache since pagination keys are dynamic
            
            logger.info(f"Deleted expert {expert_name} ({identifier_name}: {identifier}) from database")
            return {"message": f"Expert {expert_name} deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete expert {identifier}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error during deletion")