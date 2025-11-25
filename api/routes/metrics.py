"""Performance metrics and monitoring endpoints"""

import time
from fastapi import APIRouter, Request
from datetime import datetime, timezone

from core.performance import performance_monitor
from core.cache import cache
from schemas.availability import MetricsResponse

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/performance", response_model=dict)
async def get_performance_metrics():
    """Get comprehensive performance metrics"""
    
    cache_stats = cache.get_stats()
    perf_summary = performance_monitor.get_performance_summary()
    
    # Calculate additional metrics
    total_cache_requests = cache_stats["hits"] + cache_stats["misses"]
    cache_efficiency = cache_stats["hit_rate_percent"] if total_cache_requests > 0 else 0
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cache_performance": {
            **cache_stats,
            "efficiency_rating": "excellent" if cache_efficiency > 80 else 
                               "good" if cache_efficiency > 60 else 
                               "needs_improvement",
            "memory_usage_percent": round(
                (cache_stats["total_size_mb"] / cache_stats["max_memory_mb"]) * 100, 2
            ) if cache_stats["max_memory_mb"] > 0 else 0
        },
        "database_performance": perf_summary.get("query_stats", {}),
        "api_performance": perf_summary.get("request_stats", {}),
        "system_health": {
            "cache_healthy": cache_stats["hit_rate_percent"] > 30,  # Above 30% hit rate is healthy
            "memory_healthy": cache_stats["total_size_mb"] < cache_stats["max_memory_mb"] * 0.9,
            "api_healthy": len([
                stats for stats in perf_summary.get("request_stats", {}).values()
                if stats.get("avg_duration", 0) < 2.0  # Under 2 second average
            ]) > 0
        }
    }


@router.get("/cache/detailed", response_model=dict)
async def get_cache_detailed_metrics():
    """Get detailed cache performance metrics"""
    
    stats = cache.get_stats()
    
    # Calculate efficiency metrics
    total_requests = stats["hits"] + stats["misses"]
    
    return {
        "cache_statistics": stats,
        "performance_analysis": {
            "total_requests": total_requests,
            "miss_rate_percent": round(
                (stats["misses"] / total_requests * 100) if total_requests > 0 else 0, 2
            ),
            "memory_efficiency": {
                "used_mb": stats["total_size_mb"],
                "available_mb": stats["max_memory_mb"],
                "usage_percent": round(
                    (stats["total_size_mb"] / stats["max_memory_mb"]) * 100, 2
                ) if stats["max_memory_mb"] > 0 else 0
            },
            "eviction_rate": stats["evictions"],
            "recommendations": generate_cache_recommendations(stats)
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@router.post("/reset")
async def reset_performance_metrics():
    """Reset performance metrics (useful for testing)"""
    performance_monitor.reset_stats()
    
    return {
        "message": "Performance metrics reset successfully",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def generate_cache_recommendations(stats: dict) -> list:
    """Generate cache optimization recommendations based on metrics"""
    recommendations = []
    
    hit_rate = stats["hit_rate_percent"]
    memory_usage = (stats["total_size_mb"] / stats["max_memory_mb"]) * 100 if stats["max_memory_mb"] > 0 else 0
    
    if hit_rate < 30:
        recommendations.append(
            "Low cache hit rate detected. Consider increasing cache TTL or reviewing cache key strategy."
        )
    
    if hit_rate > 95:
        recommendations.append(
            "Extremely high cache hit rate. Consider reducing TTL to ensure data freshness."
        )
    
    if memory_usage > 80:
        recommendations.append(
            "High memory usage detected. Consider increasing max memory limit or optimizing cached data size."
        )
    
    if stats["evictions"] > stats["hits"] * 0.1:  # More than 10% of hits
        recommendations.append(
            "High eviction rate detected. Consider increasing cache size or reducing TTL variance."
        )
    
    if not recommendations:
        recommendations.append("Cache performance is optimal.")
    
    return recommendations