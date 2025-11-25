"""Performance monitoring and optimization utilities"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """Monitor and track performance metrics"""
    
    def __init__(self):
        self.query_stats: Dict[str, Dict[str, Any]] = {}
        self.request_stats: Dict[str, Dict[str, Any]] = {}
    
    def record_query_time(self, query_type: str, duration: float, rows_affected: Optional[int] = None):
        """Record database query performance metrics"""
        if query_type not in self.query_stats:
            self.query_stats[query_type] = {
                "count": 0,
                "total_duration": 0.0,
                "avg_duration": 0.0,
                "max_duration": 0.0,
                "min_duration": float('inf'),
                "total_rows": 0
            }
        
        stats = self.query_stats[query_type]
        stats["count"] += 1
        stats["total_duration"] += duration
        stats["avg_duration"] = stats["total_duration"] / stats["count"]
        stats["max_duration"] = max(stats["max_duration"], duration)
        stats["min_duration"] = min(stats["min_duration"], duration)
        
        if rows_affected is not None:
            stats["total_rows"] += rows_affected
        
        # Log slow queries
        if duration > 1.0:  # Log queries taking more than 1 second
            logger.warning(
                f"Slow query detected: {query_type} took {duration:.2f}s, "
                f"rows: {rows_affected or 'unknown'}"
            )
    
    def record_api_request(self, endpoint: str, method: str, duration: float, status_code: int):
        """Record API request performance metrics"""
        key = f"{method} {endpoint}"
        if key not in self.request_stats:
            self.request_stats[key] = {
                "count": 0,
                "total_duration": 0.0,
                "avg_duration": 0.0,
                "max_duration": 0.0,
                "min_duration": float('inf'),
                "error_count": 0
            }
        
        stats = self.request_stats[key]
        stats["count"] += 1
        stats["total_duration"] += duration
        stats["avg_duration"] = stats["total_duration"] / stats["count"]
        stats["max_duration"] = max(stats["max_duration"], duration)
        stats["min_duration"] = min(stats["min_duration"], duration)
        
        if status_code >= 400:
            stats["error_count"] += 1
        
        # Log slow requests
        if duration > 5.0:  # Log requests taking more than 5 seconds
            logger.warning(
                f"Slow API request: {key} took {duration:.2f}s, status: {status_code}"
            )
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get current performance summary"""
        return {
            "query_stats": self.query_stats,
            "request_stats": self.request_stats,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def reset_stats(self):
        """Reset all performance statistics"""
        self.query_stats.clear()
        self.request_stats.clear()


@asynccontextmanager
async def track_query_performance(query_type: str, performance_monitor: PerformanceMonitor):
    """Context manager to track database query performance"""
    start_time = time.time()
    rows_affected = None
    
    try:
        yield
    finally:
        duration = time.time() - start_time
        performance_monitor.record_query_time(query_type, duration, rows_affected)


@asynccontextmanager
async def track_api_performance(endpoint: str, method: str, performance_monitor: PerformanceMonitor):
    """Context manager to track API request performance"""
    start_time = time.time()
    status_code = 200
    
    try:
        yield
    except Exception as e:
        status_code = getattr(e, 'status_code', 500)
        raise
    finally:
        duration = time.time() - start_time
        performance_monitor.record_api_performance(endpoint, method, duration, status_code)


class DatabaseIndexOptimizer:
    """Utility for managing database indexes for performance"""
    
    @staticmethod
    async def ensure_performance_indexes():
        """Ensure all performance-critical indexes exist"""
        from tortoise import Tortoise
        
        connection = Tortoise.get_connection("default")
        
        indexes_to_create = [
            # Expert table indexes
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_experts_updated_at ON experts(updated_at DESC)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_experts_last_availability_check ON experts(last_availability_check DESC)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_experts_cronofy_id_calendar ON experts(cronofy_id) INCLUDE (calendar_ids)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_experts_bubble_uid_version ON experts(bubble_uid) INCLUDE (version)",
            
            # Availability errors table indexes
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_availability_errors_error_reason ON availability_errors(error_reason)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_availability_errors_unix_timestamp ON availability_errors(unix_timestamp DESC)",
        ]
        
        for index_sql in indexes_to_create:
            try:
                logger.info(f"Creating index: {index_sql.split('idx_')[1].split(' ')[0]}")
                await connection.execute_query(index_sql)
                logger.info("Index created successfully")
            except Exception as e:
                # Index might already exist
                if "already exists" in str(e).lower():
                    logger.debug(f"Index already exists: {e}")
                else:
                    logger.error(f"Failed to create index: {e}")
    
    @staticmethod
    async def analyze_table_performance():
        """Analyze table performance and suggest optimizations"""
        from tortoise import Tortoise
        
        connection = Tortoise.get_connection("default")
        
        # Analyze experts table
        try:
            stats_query = """
            SELECT 
                schemaname,
                tablename,
                attname,
                n_distinct,
                correlation
            FROM pg_stats 
            WHERE tablename IN ('experts', 'availability_errors')
            ORDER BY tablename, attname;
            """
            
            result = await connection.execute_query(stats_query)
            logger.info(f"Table statistics: {result}")
            
            # Check for unused indexes
            unused_indexes_query = """
            SELECT 
                schemaname, 
                tablename, 
                indexname, 
                idx_scan, 
                idx_tup_read, 
                idx_tup_fetch
            FROM pg_stat_user_indexes 
            WHERE idx_scan = 0
            AND tablename IN ('experts', 'availability_errors');
            """
            
            unused_result = await connection.execute_query(unused_indexes_query)
            if unused_result:
                logger.warning(f"Unused indexes found: {unused_result}")
            
        except Exception as e:
            logger.error(f"Failed to analyze table performance: {e}")


# Global performance monitor instance
performance_monitor = PerformanceMonitor()