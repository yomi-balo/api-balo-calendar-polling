#!/usr/bin/env python3
"""
Migration script to add availability_errors table for tracking expert availability failures
"""

import asyncio
import logging
from tortoise import Tortoise
from config.settings import settings

logger = logging.getLogger(__name__)


async def run_migration():
    """Add availability_errors table"""
    # Initialize database connection
    await Tortoise.init(
        db_url=settings.DATABASE_URL,
        modules={'models': ['models.expert', 'models.availability_error']}
    )
    
    try:
        # Get database connection
        connection = Tortoise.get_connection("default")
        
        # Check if availability_errors table already exists
        try:
            result = await connection.execute_query(
                "SELECT COUNT(*) FROM availability_errors LIMIT 1"
            )
            logger.info("availability_errors table already exists, skipping migration")
            return
        except Exception:
            logger.info("availability_errors table doesn't exist, proceeding with migration")
        
        # Read and execute the SQL migration
        with open('migrations/002_add_availability_errors_table.sql', 'r') as f:
            sql_commands = f.read()
        
        # Split by semicolon and execute each command
        for command in sql_commands.split(';'):
            command = command.strip()
            if command:
                await connection.execute_query(command)
        
        logger.info("Created availability_errors table with indexes")
        logger.info("Migration completed successfully")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        await Tortoise.close_connections()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_migration())