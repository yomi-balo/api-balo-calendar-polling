import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config.settings import settings
from services.expert_service import ExpertService

logger = logging.getLogger(__name__)


class SchedulerService:
    """Service for managing background scheduled tasks"""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()

    def start(self):
        """Start the scheduler with availability update job"""
        self.scheduler.add_job(
            ExpertService.update_all_expert_availability,
            "interval",
            minutes=settings.AVAILABILITY_UPDATE_INTERVAL_MINUTES,
            id="update_availability",
            replace_existing=True
        )

        self.scheduler.start()
        logger.info(f"Scheduler started with {settings.AVAILABILITY_UPDATE_INTERVAL_MINUTES}-minute interval")

    def shutdown(self):
        """Shutdown the scheduler"""
        self.scheduler.shutdown()
        logger.info("Scheduler shut down")

    def is_running(self) -> bool:
        """Check if scheduler is running"""
        return self.scheduler.running if self.scheduler else False


# Global scheduler instance
scheduler_service = SchedulerService()