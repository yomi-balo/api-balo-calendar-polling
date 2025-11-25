"""Enhanced in-memory cache with TTL support and performance optimizations"""

import asyncio
import time
import logging
import hashlib
import json
from typing import Any, Optional, Dict, List, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    value: Any
    expires_at: float
    created_at: float
    access_count: int = 0
    last_accessed: float = 0.0
    size_bytes: int = 0


class EnhancedCache:
    """Enhanced in-memory cache with TTL support, LRU eviction, and performance optimizations"""
    
    def __init__(self, default_ttl: int = 300, max_size: int = 1000, max_memory_mb: int = 100):
        self._cache: Dict[str, CacheEntry] = {}
        self._default_ttl = default_ttl
        self._max_size = max_size
        self._max_memory_bytes = max_memory_mb * 1024 * 1024  # Convert MB to bytes
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = True
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "total_size_bytes": 0
        }
    
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
        """Get value from cache with access tracking"""
        entry = self._cache.get(key)
        if entry is None:
            self._stats["misses"] += 1
            return None
        
        current_time = time.time()
        if current_time > entry.expires_at:
            # Entry expired, remove it
            await self._remove_entry(key)
            self._stats["misses"] += 1
            return None
        
        # Update access statistics
        entry.access_count += 1
        entry.last_accessed = current_time
        self._stats["hits"] += 1
        
        return entry.value
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache with TTL and size management"""
        if ttl is None:
            ttl = self._default_ttl
        
        # Estimate size of the cached object
        size_bytes = self._estimate_size(value)
        current_time = time.time()
        expires_at = current_time + ttl
        
        # Check if we need to evict entries before adding
        await self._ensure_capacity(size_bytes)
        
        # Remove existing entry if present to update stats
        if key in self._cache:
            await self._remove_entry(key)
        
        entry = CacheEntry(
            value=value,
            expires_at=expires_at,
            created_at=current_time,
            access_count=0,
            last_accessed=current_time,
            size_bytes=size_bytes
        )
        
        self._cache[key] = entry
        self._stats["total_size_bytes"] += size_bytes
        logger.debug(f"Cached key '{key}' with TTL {ttl}s, size: {size_bytes} bytes")
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache"""
        return await self._remove_entry(key)
    
    async def clear(self) -> None:
        """Clear all cache entries"""
        self._cache.clear()
        self._stats["total_size_bytes"] = 0
        logger.debug("Cache cleared")
    
    def size(self) -> int:
        """Get current cache size"""
        return len(self._cache)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        total_requests = self._stats["hits"] + self._stats["misses"]
        hit_rate = (self._stats["hits"] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            "size": len(self._cache),
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "hit_rate_percent": round(hit_rate, 2),
            "evictions": self._stats["evictions"],
            "total_size_bytes": self._stats["total_size_bytes"],
            "total_size_mb": round(self._stats["total_size_bytes"] / (1024 * 1024), 2),
            "max_size": self._max_size,
            "max_memory_mb": self._max_memory_bytes // (1024 * 1024)
        }
    
    async def _remove_entry(self, key: str) -> bool:
        """Remove entry from cache and update stats"""
        if key in self._cache:
            entry = self._cache[key]
            self._stats["total_size_bytes"] -= entry.size_bytes
            del self._cache[key]
            return True
        return False
    
    def _estimate_size(self, obj: Any) -> int:
        """Estimate the size of a cached object in bytes"""
        try:
            # For simple estimation, serialize to JSON and get byte length
            json_str = json.dumps(obj, default=str)
            return len(json_str.encode('utf-8'))
        except (TypeError, ValueError):
            # Fallback for non-serializable objects
            return len(str(obj).encode('utf-8'))
    
    async def _ensure_capacity(self, new_entry_size: int) -> None:
        """Ensure cache has capacity for new entry, evict if necessary"""
        # Check size limit
        while (len(self._cache) >= self._max_size or 
               self._stats["total_size_bytes"] + new_entry_size > self._max_memory_bytes):
            await self._evict_least_recently_used()
    
    async def _evict_least_recently_used(self) -> None:
        """Evict the least recently used entry"""
        if not self._cache:
            return
        
        # Find LRU entry
        lru_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].last_accessed
        )
        
        await self._remove_entry(lru_key)
        self._stats["evictions"] += 1
        logger.debug(f"Evicted LRU cache entry: {lru_key}")
    
    async def create_smart_key(self, *args, **kwargs) -> str:
        """Create a deterministic cache key from arguments"""
        key_data = {
            "args": args,
            "kwargs": sorted(kwargs.items()) if kwargs else {}
        }
        
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_str.encode()).hexdigest()
    
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
                    await self._remove_entry(key)
                
                if expired_keys:
                    logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
                
                # Run cleanup every 60 seconds
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cache cleanup: {str(e)}")
                await asyncio.sleep(60)


# Compatibility alias for existing code
SimpleCache = EnhancedCache

# Global cache instance with Railway-optimized settings
cache = EnhancedCache(
    default_ttl=300,  # 5 minutes default TTL
    max_size=1000,    # Max 1000 cached items
    max_memory_mb=50  # Max 50MB cache memory for Railway
)