from functools import lru_cache
import aioredis
from typing import Optional, Any, Dict, List
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from collections import defaultdict
import time

class Cache:
    def __init__(
        self, 
        redis_url: Optional[str] = None,
        max_memory_items: int = 1000,
        executor_workers: int = 4
    ):
        self.redis = aioredis.from_url(redis_url) if redis_url else None
        self.memory_cache: Dict[str, Any] = {}
        self.max_memory_items = max_memory_items
        self.executor = ThreadPoolExecutor(max_workers=executor_workers)
        self.lock = asyncio.Lock()
        
    async def get(self, key: str) -> Optional[Any]:
        """Get value with memory cache first, then Redis"""
        # Check memory cache first
        if key in self.memory_cache:
            value, expire_time = self.memory_cache[key]
            if expire_time > time.time():
                return value
            else:
                del self.memory_cache[key]
        
        # Try Redis if available
        if self.redis:
            value = await self.redis.get(key)
            if value:
                try:
                    decoded = json.loads(value)
                    await self._update_memory_cache(key, decoded)
                    return decoded
                except json.JSONDecodeError:
                    return None
        return None
        
    async def set(
        self, 
        key: str, 
        value: Any, 
        expire: int = 3600,
        memory_only: bool = False
    ):
        """Set value in both memory and Redis cache"""
        async with self.lock:
            expire_time = time.time() + expire
            
            # Update memory cache
            await self._update_memory_cache(key, value, expire_time)
            
            # Update Redis if available and not memory_only
            if self.redis and not memory_only:
                try:
                    await self.redis.set(
                        key,
                        json.dumps(value),
                        ex=expire
                    )
                except Exception as e:
                    print(f"Redis set error: {e}")
    
    async def _update_memory_cache(
        self, 
        key: str, 
        value: Any, 
        expire_time: Optional[float] = None
    ):
        """Update memory cache with LRU eviction"""
        if len(self.memory_cache) >= self.max_memory_items:
            # Evict oldest item
            oldest_key = min(
                self.memory_cache.items(),
                key=lambda x: x[1][1]
            )[0]
            del self.memory_cache[oldest_key]
        
        if expire_time is None:
            expire_time = time.time() + 3600
            
        self.memory_cache[key] = (value, expire_time)
    
    @lru_cache(maxsize=100)
    def get_cached(self, key: str) -> Optional[Any]:
        """Fast memory-only cache for frequently accessed data"""
        if key in self.memory_cache:
            value, expire_time = self.memory_cache[key]
            if expire_time > time.time():
                return value
        return None
    
    async def delete(self, key: str):
        """Delete from both caches"""
        if key in self.memory_cache:
            del self.memory_cache[key]
        if self.redis:
            await self.redis.delete(key)
            
    async def clear(self):
        """Clear all caches"""
        self.memory_cache.clear()
        if self.redis:
            await self.redis.flushdb()

class PerformanceMonitor:
    def __init__(self):
        self.metrics = defaultdict(list)
        self.lock = asyncio.Lock()

    async def track(self, metric_name: str, duration: float):
        async with self.lock:
            self.metrics[metric_name].append(duration)
            if len(self.metrics[metric_name]) > 100:
                self.metrics[metric_name].pop(0)

    def get_percentile(self, metric_name: str, percentile: float):
        data = sorted(self.metrics.get(metric_name, []))
        index = int(len(data) * percentile)
        return data[index] if data else 0

async def process_book_batch(
    client, 
    book_batch: List[Dict], 
    database_id: str,
    latest_sort: int,
    batch_size: int = 5
):
    """Process books in parallel batches"""
    semaphore = asyncio.Semaphore(batch_size)
    
    async def process_with_semaphore(book):
        async with semaphore:
            return await process_single_book(
                client, book, database_id, latest_sort
            )
    
    tasks = [
        process_with_semaphore(book) 
        for book in book_batch
    ]
    return await asyncio.gather(*tasks, return_exceptions=True) 