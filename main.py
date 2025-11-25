import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from config.settings import settings
from config.database import init_database, close_database
from core.scheduler import scheduler_service
from services.expert_service import ExpertService
from services.cronofy_service import CronofyService
from core.cache import cache
from api.routes import experts, health
from core.middleware import PerformanceTrackingMiddleware

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

    # Start cache cleanup task
    await cache.start_cleanup_task()

    # Initialize database performance optimizations
    try:
        from core.performance import DatabaseIndexOptimizer
        await DatabaseIndexOptimizer.ensure_performance_indexes()
        logger.info("Database performance indexes ensured")
    except Exception as e:
        logger.error(f"Database index optimization failed: {e}")

    # Run initial availability update
    try:
        await ExpertService.update_all_expert_availability()
    except Exception as e:
        logger.error(f"Initial availability update failed: {e}")

    logger.info(f"{settings.APP_NAME} startup complete")

    yield

    # Graceful Shutdown
    logger.info("Starting graceful shutdown...")
    
    try:
        # Stop scheduler first to prevent new tasks
        logger.info("Stopping scheduler...")
        scheduler_service.shutdown()
        
        # Stop cache cleanup task
        logger.info("Stopping cache cleanup...")
        await cache.stop_cleanup_task()
        
        # Close HTTP clients gracefully
        logger.info("Closing HTTP clients...")
        await CronofyService.close_client()
        
        # Close database connections last
        logger.info("Closing database connections...")
        await close_database()
        
        logger.info("Graceful shutdown completed successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
        # Force cleanup even if graceful shutdown fails
        try:
            await CronofyService.close_client()
            await close_database()
        except:
            pass
        logger.info("Forced shutdown completed")


# Create FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan
)

# Add performance tracking middleware
app.add_middleware(PerformanceTrackingMiddleware)

# Include routers
app.include_router(health.router)
app.include_router(experts.router)

# Add metrics router for performance monitoring
from api.routes import metrics
app.include_router(metrics.router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)