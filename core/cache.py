"""Simple in-memory cache with TTL support"""

import asyncio
import time
import logging
from typing import Any, Optional, Dict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    value: Any
    expires_at: float


class SimpleCache:
    """Simple in-memory cache with TTL support"""
    
    def __init__(self, default_ttl: int = 300):  # 5 minutes default TTL
        self._cache: Dict[str, CacheEntry] = {}
        self._default_ttl = default_ttl
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = True
    
    async def start_cleanup_task(self):
        """Start background cleanup task"""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_expired())
    
    async def stop_cleanup_task(self):
        """Stop background cleanup task"""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        entry = self._cache.get(key)
        if entry is None:
            return None
        
        if time.time() > entry.expires_at:
            # Entry expired, remove it
            del self._cache[key]
            return None
        
        return entry.value
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache with TTL"""
        if ttl is None:
            ttl = self._default_ttl
        
        expires_at = time.time() + ttl
        self._cache[key] = CacheEntry(value=value, expires_at=expires_at)
        logger.debug(f"Cached key '{key}' with TTL {ttl}s")
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache"""
        if key in self._cache:
            del self._cache[key]
            return True
        return False
    
    async def clear(self) -> None:
        """Clear all cache entries"""
        self._cache.clear()
        logger.debug("Cache cleared")
    
    def size(self) -> int:
        """Get current cache size"""
        return len(self._cache)
    
    async def _cleanup_expired(self):
        """Background task to clean up expired entries"""
        while self._running:
            try:
                current_time = time.time()
                expired_keys = [
                    key for key, entry in self._cache.items()
                    if current_time > entry.expires_at
                ]
                
                for key in expired_keys:
                    del self._cache[key]
                
                if expired_keys:
                    logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
                
                # Run cleanup every 60 seconds
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cache cleanup: {str(e)}")
                await asyncio.sleep(60)


# Global cache instance
cache = SimpleCache(default_ttl=300)  # 5 minutes default TTL