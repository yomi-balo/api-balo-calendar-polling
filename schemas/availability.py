from pydantic import BaseModel
from typing import Optional

class AvailabilityData(BaseModel):
    """Schema for availability data"""
    expert_id: str
    earliest_available_unix: Optional[int]
    last_updated: str

class HealthResponse(BaseModel):
    """Schema for health check response"""
    status: str
    experts_in_database: Optional[int] = None
    scheduler_running: bool
    database_connected: bool
    database_url_set: bool
    cronofy_token_set: bool
    algolia_configured: bool
    cache_enabled: bool
    cache_size: int
    app_version: str
    uptime_seconds: Optional[float] = None
    last_availability_update: Optional[str] = None
    recently_updated_experts: Optional[int] = None
    timestamp: str
    error: Optional[str] = None