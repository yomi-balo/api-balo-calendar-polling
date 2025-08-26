from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class ExpertCalendar(BaseModel):
    """Schema for creating/updating expert calendar mappings"""
    expert_name: str
    cronofy_id: str
    calendar_ids: List[str]
    bubble_uid: str

class ExpertCalendarList(BaseModel):
    """Schema for bulk expert calendar operations"""
    experts: List[ExpertCalendar]

class ExpertUpdate(BaseModel):
    """Schema for updating existing expert"""
    cronofy_id: str
    calendar_ids: List[str]

class ExpertResponse(BaseModel):
    """Schema for expert details response"""
    expert_name: str
    cronofy_id: str
    calendar_ids: List[str]
    bubble_uid: str
    created_at: datetime
    updated_at: datetime
    last_availability_check: Optional[datetime] = None
    earliest_available_unix: Optional[int] = None

class ExpertListResponse(BaseModel):
    """Schema for expert list response"""
    experts: List[dict]
    total_count: int

class ExpertCreateResponse(BaseModel):
    """Schema for expert creation response"""
    message: str
    updated_count: int