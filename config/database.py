import logging
from tortoise import Tortoise
from config.settings import settings

logger = logging.getLogger(__name__)


async def init_database():
    """Initialize Tortoise ORM database connection"""
    settings.validate()

    try:
        await Tortoise.init(
            db_url=settings.DATABASE_URL,
            modules={'models': ['models.expert']}
        )
        await Tortoise.generate_schemas()
        logger.info("Database connection established successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


async def close_database():
    """Close database connections"""
    await Tortoise.close_connections()
    logger.info("Database connections closed")