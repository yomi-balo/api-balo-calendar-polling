from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class AvailabilityErrorResponse(BaseModel):
    """Schema for availability error response"""
    bubble_uid: str
    expert_name: str
    cronofy_id: str
    error_reason: str
    error_details: Optional[str]
    unix_timestamp: int
    melbourne_time: str
    created_at: datetime
    updated_at: datetime


class AvailabilityErrorListResponse(BaseModel):
    """Schema for availability error list response"""
    errors: List[AvailabilityErrorResponse]
    total_count: int
    message: str