"""Unit tests for EnhancedCache"""

import pytest
import asyncio
import time
from unittest.mock import patch, MagicMock

from core.cache import EnhancedCache, CacheEntry


# =============================================================================
# CacheEntry Tests
# =============================================================================

class TestCacheEntry:
    """Tests for CacheEntry dataclass"""

    def test_default_values(self):
        """CacheEntry should have correct defaults"""
        entry = CacheEntry(
            value="test",
            expires_at=1000.0,
            created_at=900.0
        )
        assert entry.value == "test"
        assert entry.expires_at == 1000.0
        assert entry.created_at == 900.0
        assert entry.access_count == 0
        assert entry.last_accessed == 0.0
        assert entry.size_bytes == 0

    def test_custom_values(self):
        """CacheEntry should accept all custom values"""
        entry = CacheEntry(
            value={"data": [1, 2, 3]},
            expires_at=2000.0,
            created_at=1000.0,
            access_count=5,
            last_accessed=1500.0,
            size_bytes=100
        )
        assert entry.value == {"data": [1, 2, 3]}
        assert entry.access_count == 5
        assert entry.size_bytes == 100


# =============================================================================
# EnhancedCache Basic Operations Tests
# =============================================================================

class TestEnhancedCacheBasicOperations:
    """Tests for basic cache get/set/delete operations"""

    @pytest.fixture
    def cache(self):
        """Create a fresh cache for each test"""
        return EnhancedCache(default_ttl=60, max_size=100, max_memory_mb=10)

    @pytest.mark.asyncio
    async def test_set_and_get(self, cache):
        """Should store and retrieve values"""
        await cache.set("key1", "value1")
        result = await cache.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_get_nonexistent_key_returns_none(self, cache):
        """Should return None for missing keys"""
        result = await cache.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_overwrites_existing(self, cache):
        """Should overwrite existing values"""
        await cache.set("key1", "value1")
        await cache.set("key1", "value2")
        result = await cache.get("key1")
        assert result == "value2"

    @pytest.mark.asyncio
    async def test_delete_existing_key(self, cache):
        """Should delete existing keys"""
        await cache.set("key1", "value1")
        deleted = await cache.delete("key1")
        assert deleted is True
        result = await cache.get("key1")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_key(self, cache):
        """Should return False when deleting nonexistent key"""
        deleted = await cache.delete("nonexistent")
        assert deleted is False

    @pytest.mark.asyncio
    async def test_clear_removes_all(self, cache):
        """Should remove all entries"""
        await cache.set("key1", "value1")
        await cache.set("key2", "value2")
        await cache.set("key3", "value3")

        await cache.clear()

        assert cache.size() == 0
        assert await cache.get("key1") is None
        assert await cache.get("key2") is None

    @pytest.mark.asyncio
    async def test_size_returns_count(self, cache):
        """Should return correct entry count"""
        assert cache.size() == 0

        await cache.set("key1", "value1")
        assert cache.size() == 1

        await cache.set("key2", "value2")
        assert cache.size() == 2

        await cache.delete("key1")
        assert cache.size() == 1

    @pytest.mark.asyncio
    async def test_stores_complex_objects(self, cache):
        """Should store and retrieve complex objects"""
        complex_value = {
            "name": "test",
            "numbers": [1, 2, 3],
            "nested": {"a": 1, "b": 2}
        }
        await cache.set("complex", complex_value)
        result = await cache.get("complex")
        assert result == complex_value


# =============================================================================
# TTL (Time-To-Live) Tests
# =============================================================================

class TestEnhancedCacheTTL:
    """Tests for cache TTL behavior"""

    @pytest.fixture
    def cache(self):
        """Create a cache with short TTL for testing"""
        return EnhancedCache(default_ttl=1, max_size=100, max_memory_mb=10)

    @pytest.mark.asyncio
    async def test_default_ttl_applied(self, cache):
        """Should use default TTL when not specified"""
        await cache.set("key1", "value1")
        # Immediately should be available
        result = await cache.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_custom_ttl_applied(self, cache):
        """Should use custom TTL when specified"""
        await cache.set("key1", "value1", ttl=5)
        result = await cache.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_expired_entry_returns_none(self, cache):
        """Should return None for expired entries"""
        await cache.set("key1", "value1", ttl=0)  # Expire immediately
        # Small delay to ensure expiration
        await asyncio.sleep(0.01)
        result = await cache.get("key1")
        assert result is None

    @pytest.mark.asyncio
    async def test_expired_entry_removed_on_access(self, cache):
        """Should remove expired entry when accessed"""
        await cache.set("key1", "value1", ttl=0)
        await asyncio.sleep(0.01)

        # Access should trigger removal
        await cache.get("key1")
        assert cache.size() == 0


# =============================================================================
# LRU Eviction Tests
# =============================================================================

class TestEnhancedCacheLRU:
    """Tests for LRU eviction behavior"""

    @pytest.fixture
    def small_cache(self):
        """Create a small cache to test eviction"""
        return EnhancedCache(default_ttl=300, max_size=3, max_memory_mb=10)

    @pytest.mark.asyncio
    async def test_evicts_when_max_size_reached(self, small_cache):
        """Should evict when max_size is reached"""
        await small_cache.set("key1", "value1")
        await small_cache.set("key2", "value2")
        await small_cache.set("key3", "value3")

        # This should trigger eviction
        await small_cache.set("key4", "value4")

        assert small_cache.size() <= 3

    @pytest.mark.asyncio
    async def test_evicts_least_recently_used(self, small_cache):
        """Should evict the least recently used entry"""
        await small_cache.set("key1", "value1")
        await asyncio.sleep(0.01)
        await small_cache.set("key2", "value2")
        await asyncio.sleep(0.01)
        await small_cache.set("key3", "value3")

        # Access key1 to make it recently used
        await small_cache.get("key1")
        await asyncio.sleep(0.01)

        # Add key4 - should evict key2 (least recently used)
        await small_cache.set("key4", "value4")

        # key1 should still exist (was accessed)
        assert await small_cache.get("key1") == "value1"
        # key4 should exist (just added)
        assert await small_cache.get("key4") == "value4"

    @pytest.mark.asyncio
    async def test_eviction_increments_stats(self, small_cache):
        """Should track eviction count in stats"""
        await small_cache.set("key1", "value1")
        await small_cache.set("key2", "value2")
        await small_cache.set("key3", "value3")

        initial_evictions = small_cache.get_stats()["evictions"]

        await small_cache.set("key4", "value4")

        stats = small_cache.get_stats()
        assert stats["evictions"] > initial_evictions


# =============================================================================
# Memory Limit Tests
# =============================================================================

class TestEnhancedCacheMemoryLimit:
    """Tests for memory-based eviction"""

    @pytest.fixture
    def memory_limited_cache(self):
        """Create a cache with very small memory limit"""
        # 1KB memory limit
        cache = EnhancedCache(default_ttl=300, max_size=1000, max_memory_mb=0)
        cache._max_memory_bytes = 500  # 500 bytes for testing
        return cache

    @pytest.mark.asyncio
    async def test_evicts_when_memory_limit_exceeded(self, memory_limited_cache):
        """Should evict when memory limit is exceeded"""
        # Add entries until we exceed memory
        large_value = "x" * 100  # ~100 bytes per entry

        await memory_limited_cache.set("key1", large_value)
        await memory_limited_cache.set("key2", large_value)
        await memory_limited_cache.set("key3", large_value)
        await memory_limited_cache.set("key4", large_value)
        await memory_limited_cache.set("key5", large_value)

        stats = memory_limited_cache.get_stats()
        # Should have evicted some entries to stay under limit
        assert stats["total_size_bytes"] <= 500 + 150  # Allow some margin


# =============================================================================
# Statistics Tests
# =============================================================================

class TestEnhancedCacheStats:
    """Tests for cache statistics"""

    @pytest.fixture
    def cache(self):
        return EnhancedCache(default_ttl=60, max_size=100, max_memory_mb=10)

    @pytest.mark.asyncio
    async def test_tracks_hits(self, cache):
        """Should track cache hits"""
        await cache.set("key1", "value1")

        await cache.get("key1")
        await cache.get("key1")
        await cache.get("key1")

        stats = cache.get_stats()
        assert stats["hits"] == 3

    @pytest.mark.asyncio
    async def test_tracks_misses(self, cache):
        """Should track cache misses"""
        await cache.get("nonexistent1")
        await cache.get("nonexistent2")

        stats = cache.get_stats()
        assert stats["misses"] == 2

    @pytest.mark.asyncio
    async def test_calculates_hit_rate(self, cache):
        """Should calculate hit rate percentage"""
        await cache.set("key1", "value1")

        # 2 hits
        await cache.get("key1")
        await cache.get("key1")
        # 2 misses
        await cache.get("missing1")
        await cache.get("missing2")

        stats = cache.get_stats()
        assert stats["hit_rate_percent"] == 50.0

    @pytest.mark.asyncio
    async def test_hit_rate_zero_when_no_requests(self, cache):
        """Should return 0 hit rate when no requests made"""
        stats = cache.get_stats()
        assert stats["hit_rate_percent"] == 0

    @pytest.mark.asyncio
    async def test_tracks_total_size(self, cache):
        """Should track total size in bytes"""
        await cache.set("key1", "small")
        await cache.set("key2", "a bit larger value")

        stats = cache.get_stats()
        assert stats["total_size_bytes"] > 0

    @pytest.mark.asyncio
    async def test_stats_includes_all_fields(self, cache):
        """Should include all expected stats fields"""
        stats = cache.get_stats()

        expected_fields = [
            "size", "hits", "misses", "hit_rate_percent",
            "evictions", "total_size_bytes", "total_size_mb",
            "max_size", "max_memory_mb"
        ]

        for field in expected_fields:
            assert field in stats, f"Missing field: {field}"


# =============================================================================
# Access Tracking Tests
# =============================================================================

class TestEnhancedCacheAccessTracking:
    """Tests for access count and last_accessed tracking"""

    @pytest.fixture
    def cache(self):
        return EnhancedCache(default_ttl=60, max_size=100, max_memory_mb=10)

    @pytest.mark.asyncio
    async def test_increments_access_count(self, cache):
        """Should increment access_count on each get"""
        await cache.set("key1", "value1")

        await cache.get("key1")
        await cache.get("key1")
        await cache.get("key1")

        entry = cache._cache["key1"]
        assert entry.access_count == 3

    @pytest.mark.asyncio
    async def test_updates_last_accessed(self, cache):
        """Should update last_accessed timestamp on get"""
        await cache.set("key1", "value1")
        initial_accessed = cache._cache["key1"].last_accessed

        await asyncio.sleep(0.01)
        await cache.get("key1")

        entry = cache._cache["key1"]
        assert entry.last_accessed > initial_accessed


# =============================================================================
# Smart Key Generation Tests
# =============================================================================

class TestEnhancedCacheSmartKey:
    """Tests for smart key generation"""

    @pytest.fixture
    def cache(self):
        return EnhancedCache(default_ttl=60, max_size=100, max_memory_mb=10)

    @pytest.mark.asyncio
    async def test_creates_deterministic_key(self, cache):
        """Should create same key for same inputs"""
        key1 = await cache.create_smart_key("arg1", "arg2", param="value")
        key2 = await cache.create_smart_key("arg1", "arg2", param="value")

        assert key1 == key2

    @pytest.mark.asyncio
    async def test_different_args_different_keys(self, cache):
        """Should create different keys for different inputs"""
        key1 = await cache.create_smart_key("arg1")
        key2 = await cache.create_smart_key("arg2")

        assert key1 != key2

    @pytest.mark.asyncio
    async def test_different_kwargs_different_keys(self, cache):
        """Should create different keys for different kwargs"""
        key1 = await cache.create_smart_key(param="value1")
        key2 = await cache.create_smart_key(param="value2")

        assert key1 != key2

    @pytest.mark.asyncio
    async def test_key_is_hash_string(self, cache):
        """Should return MD5 hash string"""
        key = await cache.create_smart_key("test")

        # MD5 hash is 32 hex characters
        assert len(key) == 32
        assert all(c in "0123456789abcdef" for c in key)


# =============================================================================
# Size Estimation Tests
# =============================================================================

class TestEnhancedCacheSizeEstimation:
    """Tests for _estimate_size method"""

    @pytest.fixture
    def cache(self):
        return EnhancedCache(default_ttl=60, max_size=100, max_memory_mb=10)

    def test_estimates_string_size(self, cache):
        """Should estimate string size in bytes"""
        size = cache._estimate_size("hello")
        assert size > 0
        assert size == len('"hello"'.encode('utf-8'))

    def test_estimates_dict_size(self, cache):
        """Should estimate dict size"""
        size = cache._estimate_size({"key": "value"})
        assert size > 0

    def test_estimates_list_size(self, cache):
        """Should estimate list size"""
        size = cache._estimate_size([1, 2, 3, 4, 5])
        assert size > 0

    def test_larger_objects_have_larger_size(self, cache):
        """Larger objects should have larger estimated size"""
        small_size = cache._estimate_size("small")
        large_size = cache._estimate_size("a" * 1000)

        assert large_size > small_size


# =============================================================================
# Cleanup Task Tests
# =============================================================================

class TestEnhancedCacheCleanup:
    """Tests for background cleanup task"""

    @pytest.fixture
    def cache(self):
        return EnhancedCache(default_ttl=60, max_size=100, max_memory_mb=10)

    @pytest.mark.asyncio
    async def test_start_cleanup_task(self, cache):
        """Should start cleanup task"""
        await cache.start_cleanup_task()

        assert cache._cleanup_task is not None
        assert not cache._cleanup_task.done()

        await cache.stop_cleanup_task()

    @pytest.mark.asyncio
    async def test_stop_cleanup_task(self, cache):
        """Should stop cleanup task gracefully"""
        await cache.start_cleanup_task()
        await cache.stop_cleanup_task()

        assert cache._cleanup_task is None
        assert cache._running is False

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self, cache):
        """Should handle stop when task not started"""
        # Should not raise
        await cache.stop_cleanup_task()
        assert cache._cleanup_task is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
