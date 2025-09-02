from pydantic import BaseModel, validator, Field
from typing import List, Optional
from datetime import datetime

from core.validators import validate_and_clean_string, validate_and_clean_calendar_ids

class ExpertCalendar(BaseModel):
    """Schema for creating/updating expert calendar mappings"""
    expert_name: str = Field(..., min_length=1, max_length=255, description="Expert name")
    cronofy_id: str = Field(..., min_length=1, max_length=255, description="Cronofy ID")
    calendar_ids: List[str] = Field(..., min_items=1, description="List of calendar IDs")
    bubble_uid: str = Field(..., min_length=1, max_length=255, description="Bubble UID")

    @validator('expert_name')
    def expert_name_must_not_be_empty(cls, v):
        return validate_and_clean_string(v, 'Expert name')

    @validator('cronofy_id')
    def cronofy_id_must_be_valid(cls, v):
        return validate_and_clean_string(v, 'Cronofy ID')

    @validator('bubble_uid')
    def bubble_uid_must_be_valid(cls, v):
        return validate_and_clean_string(v, 'Bubble UID')

    @validator('calendar_ids')
    def calendar_ids_must_be_valid(cls, v):
        return validate_and_clean_calendar_ids(v)

class ExpertCalendarList(BaseModel):
    """Schema for bulk expert calendar operations"""
    experts: List[ExpertCalendar] = Field(..., min_items=1, max_items=100, description="List of experts")

    @validator('experts')
    def experts_must_have_unique_uids(cls, v):
        bubble_uids = [expert.bubble_uid for expert in v]
        cronofy_ids = [expert.cronofy_id for expert in v]
        
        if len(set(bubble_uids)) != len(bubble_uids):
            raise ValueError('Duplicate bubble_uid found in experts list')
        if len(set(cronofy_ids)) != len(cronofy_ids):
            raise ValueError('Duplicate cronofy_id found in experts list')
        
        return v

class ExpertUpdate(BaseModel):
    """Schema for updating existing expert"""
    cronofy_id: str = Field(..., min_length=1, max_length=255, description="Cronofy ID")
    calendar_ids: List[str] = Field(..., min_items=1, description="List of calendar IDs")

    @validator('cronofy_id')
    def cronofy_id_must_be_valid(cls, v):
        return validate_and_clean_string(v, 'Cronofy ID')

    @validator('calendar_ids')
    def calendar_ids_must_be_valid(cls, v):
        return validate_and_clean_calendar_ids(v)

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