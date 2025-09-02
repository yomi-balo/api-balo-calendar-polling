import time
from datetime import datetime, timezone
from fastapi import APIRouter

from models.expert import Expert
from schemas.availability import HealthResponse
from config.settings import settings
from services.algolia_service import algolia_service
from core.scheduler import scheduler_service
from core.cache import cache

router = APIRouter(tags=["health"])

# Store startup time for uptime calculation
_startup_time = time.time()

@router.get("/", response_model=dict)
async def root():
    """Root endpoint"""
    return {"message": f"{settings.APP_NAME} is running"}

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Enhanced health check endpoint with comprehensive system status"""
    try:
        # Get database status and expert count
        expert_count = await Expert.all().count()
        
        # Get most recent expert for last update timestamp
        most_recent_expert = await Expert.all().order_by('-last_availability_check').first()
        last_update = most_recent_expert.last_availability_check.isoformat() if most_recent_expert and most_recent_expert.last_availability_check else None
        
        # Check how many experts have been updated recently (within last hour)
        from datetime import timedelta
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        recently_updated_count = await Expert.filter(last_availability_check__gte=one_hour_ago).count() if most_recent_expert else 0
        
        # Calculate uptime
        uptime = time.time() - _startup_time

        return HealthResponse(
            status="healthy",
            experts_in_database=expert_count,
            scheduler_running=scheduler_service.is_running(),
            database_connected=True,
            database_url_set=bool(settings.DATABASE_URL),
            cronofy_token_set=settings.get_cronofy_configured(),
            algolia_configured=algolia_service.is_configured(),
            cache_enabled=True,
            cache_size=cache.size(),
            app_version=settings.APP_VERSION,
            uptime_seconds=uptime,
            last_availability_update=last_update,
            recently_updated_experts=recently_updated_count,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            scheduler_running=scheduler_service.is_running(),
            database_connected=False,
            database_url_set=bool(settings.DATABASE_URL),
            cronofy_token_set=settings.get_cronofy_configured(),
            algolia_configured=algolia_service.is_configured(),
            cache_enabled=True,
            cache_size=cache.size(),
            app_version=settings.APP_VERSION,
            uptime_seconds=time.time() - _startup_time,
            recently_updated_experts=0,
            timestamp=datetime.now(timezone.utc).isoformat(),
            error=str(e)
        )