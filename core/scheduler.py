import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config.settings import settings
from services.expert_service import ExpertService
from services.error_retry_service import ErrorRetryService

logger = logging.getLogger(__name__)


class SchedulerService:
    """Service for managing background scheduled tasks"""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()

    def start(self):
        """Start the scheduler with availability update and error retry jobs"""
        # Main availability update job
        self.scheduler.add_job(
            ExpertService.update_all_expert_availability,
            "interval",
            minutes=settings.AVAILABILITY_UPDATE_INTERVAL_MINUTES,
            id="update_availability",
            replace_existing=True
        )
        
        # Error retry job
        self.scheduler.add_job(
            ErrorRetryService.retry_failed_experts,
            "interval",
            minutes=settings.ERROR_RETRY_INTERVAL_MINUTES,
            id="retry_failed_experts",
            replace_existing=True
        )

        self.scheduler.start()
        logger.info(f"Scheduler started with {settings.AVAILABILITY_UPDATE_INTERVAL_MINUTES}-minute availability updates and {settings.ERROR_RETRY_INTERVAL_MINUTES}-minute error retries")

    def shutdown(self):
        """Shutdown the scheduler"""
        self.scheduler.shutdown()
        logger.info("Scheduler shut down")

    def is_running(self) -> bool:
        """Check if scheduler is running"""
        return self.scheduler.running if self.scheduler else False


# Global scheduler instance
scheduler_service = SchedulerService()