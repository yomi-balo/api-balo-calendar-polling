import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from config.settings import settings
from config.database import init_database, close_database
from core.scheduler import scheduler_service
from services.expert_service import ExpertService
from api.routes import experts, health

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")

    # Initialize database
    await init_database()

    # Start background scheduler
    scheduler_service.start()

    # Run initial availability update
    try:
        await ExpertService.update_all_expert_availability()
    except Exception as e:
        logger.error(f"Initial availability update failed: {e}")

    logger.info(f"{settings.APP_NAME} startup complete")

    yield

    # Shutdown
    logger.info("Shutting down application")
    scheduler_service.shutdown()
    await close_database()
    logger.info("Application shutdown complete")


# Create FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan
)

# Include routers
app.include_router(health.router)
app.include_router(experts.router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)