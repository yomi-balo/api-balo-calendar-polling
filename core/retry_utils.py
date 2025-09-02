"""Retry utilities with exponential backoff"""

import asyncio
import logging
import random
from typing import Callable, Any, Optional, Type, Tuple
from functools import wraps

logger = logging.getLogger(__name__)


async def retry_with_exponential_backoff(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_factor: float = 2.0,
    jitter: bool = True,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Any:
    """
    Retry a function with exponential backoff.
    
    Args:
        func: The async function to retry
        max_retries: Maximum number of retries
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_factor: Factor to multiply delay by each retry
        jitter: Add random jitter to delay
        exceptions: Tuple of exceptions to catch and retry on
    """
    for attempt in range(max_retries + 1):
        try:
            return await func()
        except exceptions as e:
            if attempt >= max_retries:
                logger.error(f"Function {func.__name__} failed after {max_retries} retries: {str(e)}")
                raise
            
            # Calculate delay with exponential backoff
            delay = min(base_delay * (exponential_factor ** attempt), max_delay)
            
            # Add jitter to avoid thundering herd
            if jitter:
                delay = delay * (0.5 + random.random() * 0.5)
            
            logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {delay:.2f}s")
            await asyncio.sleep(delay)


def with_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_factor: float = 2.0,
    jitter: bool = True,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """Decorator for adding retry logic to async functions"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async def _func():
                return await func(*args, **kwargs)
            
            return await retry_with_exponential_backoff(
                _func,
                max_retries=max_retries,
                base_delay=base_delay,
                max_delay=max_delay,
                exponential_factor=exponential_factor,
                jitter=jitter,
                exceptions=exceptions
            )
        return wrapper
    return decorator