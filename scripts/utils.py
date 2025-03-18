import asyncio
from typing import Callable, Any
from functools import wraps
import time

def get_heading(level, content):
    if level == 1:
        heading = "heading_1"
    elif level == 2:
        heading = "heading_2"
    else:
        heading = "heading_3"
    return {
        "type": heading,
        heading: {
            "rich_text": [
                {
                    "type": "text",
                    "text": {
                        "content": content,
                    },
                }
            ],
            "color": "default",
            "is_toggleable": False,
        },
    }


def get_table_of_contents():
    """获取目录"""
    return {"type": "table_of_contents", "table_of_contents": {"color": "default"}}


def get_title(content):
    return {"title": [{"type": "text", "text": {"content": content}}]}


def get_rich_text(content):
    return {"rich_text": [{"type": "text", "text": {"content": content}}]}


def get_url(url):
    return {"url": url}


def get_file(url):
    return {"files": [{"type": "external", "name": "Cover", "external": {"url": url}}]}


def get_multi_select(names):
    return {"multi_select": [{"name": name} for name in names]}


def get_date(start):
    return {
        "date": {
            "start": start,
            "time_zone": "Asia/Shanghai",
        }
    }


def get_icon(url):
    return {"type": "external", "external": {"url": url}}


def get_select(name):
    return {"select": {"name": name}}


def get_number(number):
    return {"number": number}


def get_quote(content):
    return {
        "type": "quote",
        "quote": {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": content},
                }
            ],
            "color": "default",
        },
    }


def get_callout(content, style, colorStyle, reviewId):
    # 根据不同的划线样式设置不同的emoji 直线type=0 背景颜色是1 波浪线是2
    emoji = "〰️"
    if style == 0:
        emoji = "💡"
    elif style == 1:
        emoji = "⭐"
    # 如果reviewId不是空说明是笔记
    if reviewId != None:
        emoji = "✍️"
    color = "default"
    # 根据划线颜色设置文字的颜色
    if colorStyle == 1:
        color = "red"
    elif colorStyle == 2:
        color = "purple"
    elif colorStyle == 3:
        color = "blue"
    elif colorStyle == 4:
        color = "green"
    elif colorStyle == 5:
        color = "yellow"
    return {
        "type": "callout",
        "callout": {
            "rich_text": [
                {
                    "type": "text",
                    "text": {
                        "content": content,
                    },
                }
            ],
            "icon": {"emoji": emoji},
            "color": color,
        },
    }

def retry_async(
    retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0
):
    """Retry decorator for async functions"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retry_count = 0
            current_delay = delay
            
            while retry_count < retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    retry_count += 1
                    if retry_count == retries:
                        raise e
                    
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
                    
        return wrapper
    return decorator

class RateLimiter:
    """Rate limiter for API calls"""
    def __init__(self, rate: int, capacity: int):
        self.rate = rate  # Tokens per second
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()

    async def acquire(self, tokens=1):
        while True:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return
            else:
                deficit = tokens - self.tokens
                wait_time = deficit / self.rate
                await asyncio.sleep(wait_time)

class LazyContentProcessor:
    def __init__(self, book_id: str, client: 'WeReadClient', cache: 'Cache'):
        self.book_id = book_id
        self.client = client
        self.cache = cache
        self._content = None
        self._chapters = None
        self._bookmarks = None
        self._processed = False
        self._processing_lock = asyncio.Lock()

    async def load(self) -> 'LazyContentProcessor':
        """Lazy load content only when needed"""
        cache_key = f"book_content:{self.book_id}"
        
        # Try to get from cache first
        self._content = await self.cache.get(cache_key)
        if not self._content:
            async with self._processing_lock:
                self._content = await self._fetch_content()
                await self.cache.set(cache_key, self._content)
        return self

    async def get_chapters(self) -> dict:
        """Lazy load chapters"""
        if not self._chapters:
            async with self._processing_lock:
                self._chapters = await self.client.get_chapter_info(self.book_id)
        return self._chapters

    async def get_bookmarks(self) -> list:
        """Lazy load bookmarks"""
        if not self._bookmarks:
            async with self._processing_lock:
                self._bookmarks = await self.client.get_bookmark_list(self.book_id)
        return self._bookmarks

    async def process(self):
        """Process content with batching and parallelization"""
        if self._processed:
            return

        async with self._processing_lock:
            if not self._content:
                await self.load()

            # Process in parallel using worker pool
            async with asyncio.TaskGroup() as tg:
                chapters_task = tg.create_task(self.get_chapters())
                bookmarks_task = tg.create_task(self.get_bookmarks())

            self._chapters = await chapters_task
            self._bookmarks = await bookmarks_task
            
            # Batch process chapters and bookmarks
            if self._chapters and self._bookmarks:
                await self._process_batch()
            
            self._processed = True

    async def _fetch_content(self) -> dict:
        """Fetch book content with retry logic"""
        return await self.client.make_request(
            "GET",
            f"{WEREAD_URL}/book/content",
            params={"bookId": self.book_id}
        )

    async def _process_batch(self, batch_size: int = 50):
        """Process content in batches"""
        if not self._bookmarks:
            return

        for i in range(0, len(self._bookmarks), batch_size):
            batch = self._bookmarks[i:i + batch_size]
            tasks = [
                self._process_bookmark(bookmark) 
                for bookmark in batch
            ]
            await asyncio.gather(*tasks)

    async def _process_bookmark(self, bookmark: dict):
        """Process individual bookmark with chapter context"""
        chapter_id = bookmark.get('chapterUid')
        if chapter_id and self._chapters:
            chapter = self._chapters.get(chapter_id)
            if chapter:
                # Enrich bookmark with chapter context
                bookmark['chapter_title'] = chapter.get('title')
                bookmark['chapter_level'] = chapter.get('level')