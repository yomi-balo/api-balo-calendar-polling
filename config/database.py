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
            "models": ["models.expert", "models.availability_error", "aerich.models"],
            "default_connection": "default",
        }
    },
}


async def run_migrations():
    """Run necessary database migrations"""
    try:
        connection = Tortoise.get_connection("default")
        
        # Migration 1: Check if version column exists
        try:
            await connection.execute_query("SELECT version FROM experts LIMIT 1")
            logger.info("Version column already exists")
        except Exception:
            logger.info("Adding version column to experts table")
            await connection.execute_query("ALTER TABLE experts ADD COLUMN version INTEGER DEFAULT 0")
            await connection.execute_query("UPDATE experts SET version = 0 WHERE version IS NULL")
            logger.info("Version column added successfully")
        
        # Migration 2: Check if availability_errors table exists
        try:
            await connection.execute_query("SELECT COUNT(*) FROM availability_errors LIMIT 1")
            logger.info("availability_errors table already exists")
        except Exception:
            logger.info("Creating availability_errors table")
            
            # Create availability_errors table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS availability_errors (
                bubble_uid VARCHAR(255) PRIMARY KEY,
                expert_name VARCHAR(255) NOT NULL,
                cronofy_id VARCHAR(255) NOT NULL,
                error_reason VARCHAR(500) NOT NULL,
                error_details TEXT,
                unix_timestamp BIGINT NOT NULL,
                melbourne_time VARCHAR(100) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            """
            await connection.execute_query(create_table_sql)
            
            # Create indexes
            await connection.execute_query("CREATE INDEX IF NOT EXISTS idx_availability_errors_updated_at ON availability_errors(updated_at)")
            await connection.execute_query("CREATE INDEX IF NOT EXISTS idx_availability_errors_cronofy_id ON availability_errors(cronofy_id)")
            
            logger.info("availability_errors table created successfully")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


async def init_database():
    """Initialize Tortoise ORM database connection"""
    settings.validate()

    try:
        await Tortoise.init(
            db_url=settings.DATABASE_URL,
            modules={'models': ['models.expert', 'models.availability_error']}
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