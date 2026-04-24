import json
import redis.asyncio as redis
from typing import Dict, Any, Optional
import os

# Initialize Redis client (typically configured centrally).
redis_client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))


def _build_revenue_cache_key(
    property_id: str,
    tenant_id: str,
    month: Optional[int],
    year: Optional[int],
) -> str:
    if month is not None and year is not None:
        period_key = f"{year:04d}-{month:02d}"
    else:
        period_key = "latest"
    return f"revenue:{tenant_id}:{property_id}:{period_key}"


async def get_revenue_summary(
    property_id: str,
    tenant_id: str,
    month: Optional[int] = None,
    year: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Fetches revenue summary, utilizing caching to improve performance.
    """
    cache_key = _build_revenue_cache_key(
        property_id=property_id,
        tenant_id=tenant_id,
        month=month,
        year=year,
    )
    
    # Try to get from cache
    cached = await redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    
    # Revenue calculation is delegated to the reservation service.
    from app.services.reservations import calculate_total_revenue
    
    # Calculate revenue
    result = await calculate_total_revenue(
        property_id=property_id,
        tenant_id=tenant_id,
        month=month,
        year=year,
    )
    
    # Cache the result for 5 minutes
    await redis_client.setex(cache_key, 300, json.dumps(result))
    
    return result
