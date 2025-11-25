"""Performance tracking and monitoring middleware"""

import time
import logging
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from core.performance import performance_monitor

logger = logging.getLogger(__name__)


class PerformanceTrackingMiddleware(BaseHTTPMiddleware):
    """Middleware to track API request performance"""
    
    def __init__(self, app):
        super().__init__(app)
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        
        # Extract endpoint info
        endpoint = request.url.path
        method = request.method
        
        try:
            # Process request
            response = await call_next(request)
            duration = time.time() - start_time
            
            # Record performance metrics
            performance_monitor.record_api_request(
                endpoint=endpoint,
                method=method,
                duration=duration,
                status_code=response.status_code
            )
            
            # Add performance headers for debugging
            response.headers["X-Process-Time"] = f"{duration:.4f}"
            response.headers["X-Timestamp"] = str(int(time.time()))
            
            # Log slow requests
            if duration > 5.0:
                logger.warning(
                    f"Slow request: {method} {endpoint} took {duration:.2f}s "
                    f"(status: {response.status_code})"
                )
            elif duration > 2.0:
                logger.info(
                    f"Medium request: {method} {endpoint} took {duration:.2f}s "
                    f"(status: {response.status_code})"
                )
            
            return response
            
        except Exception as e:
            duration = time.time() - start_time
            
            # Record error in performance metrics
            status_code = getattr(e, 'status_code', 500)
            performance_monitor.record_api_request(
                endpoint=endpoint,
                method=method,
                duration=duration,
                status_code=status_code
            )
            
            logger.error(
                f"Request error: {method} {endpoint} failed after {duration:.2f}s "
                f"with {type(e).__name__}: {str(e)}"
            )
            
            raise e


class DatabasePerformanceMiddleware:
    """Context manager for tracking database query performance"""
    
    def __init__(self, query_type: str):
        self.query_type = query_type
        self.start_time = None
    
    async def __aenter__(self):
        self.start_time = time.time()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            performance_monitor.record_query_time(self.query_type, duration)


def track_database_query(query_type: str):
    """Decorator to track database query performance"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                performance_monitor.record_query_time(query_type, duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                performance_monitor.record_query_time(f"{query_type}_error", duration)
                raise e
        return wrapper
    return decorator