#!/usr/bin/env python3
"""
Railway migration runner - can be executed via Railway CLI
Usage: railway run python railway_migrate.py
"""

import asyncio
import logging
import os
from tortoise import Tortoise

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_migration():
    """Run database migration on Railway"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    
    logger.info("Connecting to Railway PostgreSQL database...")
    
    # Initialize database connection
    await Tortoise.init(
        db_url=database_url,
        modules={'models': ['models.expert']}
    )
    
    try:
        connection = Tortoise.get_connection("default")
        
        # Check if version column already exists
        try:
            result = await connection.execute_query("SELECT version FROM experts LIMIT 1")
            logger.info("✅ Version column already exists - migration not needed")
            return
        except Exception:
            logger.info("❌ Version column doesn't exist - running migration...")
        
        # Add version column with default value
        logger.info("Adding version column...")
        await connection.execute_query("ALTER TABLE experts ADD COLUMN version INTEGER DEFAULT 0")
        
        # Update existing records
        logger.info("Updating existing records...")
        result = await connection.execute_query("UPDATE experts SET version = 0 WHERE version IS NULL")
        
        logger.info("✅ Migration completed successfully!")
        logger.info(f"Updated records with version column")
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        raise
    finally:
        await Tortoise.close_connections()
        logger.info("Database connection closed")


if __name__ == "__main__":
    print("🚀 Running Railway database migration...")
    try:
        asyncio.run(run_migration())
        print("✅ Migration completed successfully!")
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        exit(1)