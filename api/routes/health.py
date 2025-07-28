from datetime import datetime, timezone
from fastapi import APIRouter

from models.expert import Expert
from schemas.availability import HealthResponse
from config.settings import settings
from services.algolia_service import algolia_service
from core.scheduler import scheduler_service

router = APIRouter(tags=["health"])

@router.get("/", response_model=dict)
async def root():
    """Root endpoint"""
    return {"message": f"{settings.APP_NAME} is running"}

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint with system status"""
    try:
        expert_count = await Expert.all().count()

        return HealthResponse(
            status="healthy",
            experts_in_database=expert_count,
            scheduler_running=scheduler_service.is_running(),
            database_connected=True,
            database_url_set=bool(settings.DATABASE_URL),
            cronofy_token_set=settings.get_cronofy_configured(),
            algolia_configured=algolia_service.is_configured(),
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
            timestamp=datetime.now(timezone.utc).isoformat(),
            error=str(e)
        )