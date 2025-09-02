import logging
import os
from tortoise import Tortoise
from config.settings import settings

logger = logging.getLogger(__name__)

# Tortoise ORM configuration for aerich
TORTOISE_ORM = {
    "connections": {"default": settings.DATABASE_URL or "sqlite://db.sqlite3"},
    "apps": {
        "models": {
            "models": ["models.expert", "aerich.models"],
            "default_connection": "default",
        }
    },
}


async def run_migrations():
    """Run necessary database migrations"""
    try:
        connection = Tortoise.get_connection("default")
        
        # Check if version column exists
        try:
            await connection.execute_query("SELECT version FROM experts LIMIT 1")
            logger.info("Version column already exists")
        except Exception:
            logger.info("Adding version column to experts table")
            await connection.execute_query("ALTER TABLE experts ADD COLUMN version INTEGER DEFAULT 0")
            await connection.execute_query("UPDATE experts SET version = 0 WHERE version IS NULL")
            logger.info("Version column added successfully")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


async def init_database():
    """Initialize Tortoise ORM database connection"""
    settings.validate()

    try:
        await Tortoise.init(
            db_url=settings.DATABASE_URL,
            modules={'models': ['models.expert']}
        )
        await Tortoise.generate_schemas()
        
        # Run migrations after database initialization
        await run_migrations()
        
        logger.info("Database connection established successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


async def close_database():
    """Close database connections"""
    await Tortoise.close_connections()
    logger.info("Database connections closed")