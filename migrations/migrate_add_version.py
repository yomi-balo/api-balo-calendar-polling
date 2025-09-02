#!/usr/bin/env python3
"""
Migration script to add version column to experts table
"""

import asyncio
import logging
from tortoise import Tortoise
from config.settings import settings

logger = logging.getLogger(__name__)


async def run_migration():
    """Add version column to experts table"""
    # Initialize database connection
    await Tortoise.init(
        db_url=settings.DATABASE_URL,
        modules={'models': ['models.expert']}
    )
    
    try:
        # Get database connection
        connection = Tortoise.get_connection("default")
        
        # Check if version column already exists
        try:
            result = await connection.execute_query(
                "SELECT version FROM experts LIMIT 1"
            )
            logger.info("Version column already exists, skipping migration")
            return
        except Exception:
            logger.info("Version column doesn't exist, proceeding with migration")
        
        # Add version column
        await connection.execute_query(
            "ALTER TABLE experts ADD COLUMN version INTEGER DEFAULT 0"
        )
        logger.info("Added version column to experts table")
        
        # Update existing records
        result = await connection.execute_query(
            "UPDATE experts SET version = 0 WHERE version IS NULL"
        )
        logger.info(f"Updated {result} existing records with version = 0")
        
        logger.info("Migration completed successfully")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        await Tortoise.close_connections()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_migration())