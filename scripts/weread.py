import argparse
import json
import logging
import os
import re
import time
from notion_client import Client
import requests
from requests.utils import cookiejar_from_dict
from http.cookies import SimpleCookie
from datetime import datetime
import hashlib
from dotenv import load_dotenv
import os
from utils import (
    get_callout,
    get_date,
    get_file,
    get_heading,
    get_icon,
    get_multi_select,
    get_number,
    get_quote,
    get_rich_text,
    get_select,
    get_table_of_contents,
    get_title,
    get_url,
)
import asyncio
import aiohttp
from typing import Optional, Dict, List, Tuple, Any
from functools import partial
import concurrent.futures
from notion_client import AsyncClient
from functools import wraps

load_dotenv()
WEREAD_URL = "https://weread.qq.com/"
WEREAD_NOTEBOOKS_URL = "https://i.weread.qq.com/user/notebooks"
WEREAD_BOOKMARKLIST_URL = "https://i.weread.qq.com/book/bookmarklist"
WEREAD_CHAPTER_INFO = "https://i.weread.qq.com/book/chapterInfos"
WEREAD_READ_INFO_URL = "https://i.weread.qq.com/book/readinfo"
WEREAD_REVIEW_LIST_URL = "https://i.weread.qq.com/review/list"
WEREAD_BOOK_INFO = "https://i.weread.qq.com/book/info"


def parse_cookie_string(cookie_string):
    cookie = SimpleCookie()
    cookie.load(cookie_string)
    cookies_dict = {}
    cookiejar = None
    for key, morsel in cookie.items():
        cookies_dict[key] = morsel.value
        cookiejar = cookiejar_from_dict(cookies_dict, cookiejar=None, overwrite=True)
    return cookiejar


def get_bookmark_list(bookId):
    """获取我的划线"""
    params = dict(bookId=bookId)
    r = session.get(WEREAD_BOOKMARKLIST_URL, params=params)
    if r.ok:
        updated = r.json().get("updated")
        updated = sorted(
            updated,
            key=lambda x: (x.get("chapterUid", 1), int(x.get("range").split("-")[0])),
        )
        return r.json()["updated"]
    return None


def get_read_info(bookId):
    params = dict(bookId=bookId, readingDetail=1, readingBookIndex=1, finishedDate=1)
    r = session.get(WEREAD_READ_INFO_URL, params=params)
    if r.ok:
        return r.json()
    return None


def get_bookinfo(bookId):
    """获取书的详情"""
    params = dict(bookId=bookId)
    r = session.get(WEREAD_BOOK_INFO, params=params)
    isbn = ""
    if r.ok:
        data = r.json()
        isbn = data["isbn"]
        newRating = data["newRating"] / 1000
        return (isbn, newRating)
    else:
        print(f"get {bookId} book info failed")
        return ("", 0)


def get_review_list(bookId):
    """获取笔记"""
    params = dict(bookId=bookId, listType=11, mine=1, syncKey=0)
    r = session.get(WEREAD_REVIEW_LIST_URL, params=params)
    reviews = r.json().get("reviews")
    summary = list(filter(lambda x: x.get("review").get("type") == 4, reviews))
    reviews = list(filter(lambda x: x.get("review").get("type") == 1, reviews))
    reviews = list(map(lambda x: x.get("review"), reviews))
    reviews = list(map(lambda x: {**x, "markText": x.pop("content")}, reviews))
    return summary, reviews


def check(bookId):
    """检查是否已经插入过 如果已经插入了就删除"""
    time.sleep(0.3)
    filter = {"property": "BookId", "rich_text": {"equals": bookId}}
    response = client.databases.query(database_id=database_id, filter=filter)
    for result in response["results"]:
        time.sleep(0.3)
        client.blocks.delete(block_id=result["id"])


def get_chapter_info(bookId):
    """获取章节信息"""
    body = {"bookIds": [bookId], "synckeys": [0], "teenmode": 0}
    r = session.post(WEREAD_CHAPTER_INFO, json=body)
    if (
        r.ok
        and "data" in r.json()
        and len(r.json()["data"]) == 1
        and "updated" in r.json()["data"][0]
    ):
        update = r.json()["data"][0]["updated"]
        return {item["chapterUid"]: item for item in update}
    return None


def insert_to_notion(bookName, bookId, cover, sort, author, isbn, rating, categories):
    """插入到notion"""
    time.sleep(0.3)
    parent = {"database_id": database_id, "type": "database_id"}
    properties = {
        "BookName": get_title(bookName),
        "BookId": get_rich_text(bookId),
        "ISBN": get_rich_text(isbn),
        "URL": get_url(
            f"https://weread.qq.com/web/reader/{calculate_book_str_id(bookId)}"
        ),
        "Author": get_rich_text(author),
        "Sort": get_number(sort),
        "Rating": get_number(rating),
        "Cover": get_file(cover),
    }
    if categories != None:
        properties["Categories"] = get_multi_select(categories)
    read_info = get_read_info(bookId=bookId)
    if read_info != None:
        markedStatus = read_info.get("markedStatus", 0)
        readingTime = read_info.get("readingTime", 0)
        readingProgress = read_info.get("readingProgress", 0)
        format_time = ""
        hour = readingTime // 3600
        if hour > 0:
            format_time += f"{hour}时"
        minutes = readingTime % 3600 // 60
        if minutes > 0:
            format_time += f"{minutes}分"
        properties["Status"] = get_select("读完" if markedStatus == 4 else "在读")
        properties["ReadingTime"] = get_rich_text(format_time)
        properties["Progress"] = get_number(readingProgress)
        if "finishedDate" in read_info:
            properties["Date"] = get_date(
                datetime.utcfromtimestamp(read_info.get("finishedDate")).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )

    icon = get_icon(cover)
    # notion api 限制100个block
    response = client.pages.create(parent=parent, icon=icon,cover=icon, properties=properties)
    id = response["id"]
    return id


def add_children(id, children):
    results = []
    for i in range(0, len(children) // 100 + 1):
        time.sleep(0.3)
        response = client.blocks.children.append(
            block_id=id, children=children[i * 100 : (i + 1) * 100]
        )
        results.extend(response.get("results"))
    return results if len(results) == len(children) else None


def add_grandchild(grandchild, results):
    for key, value in grandchild.items():
        time.sleep(0.3)
        id = results[key].get("id")
        client.blocks.children.append(block_id=id, children=[value])


def get_notebooklist():
    """获取笔记本列表"""
    r = session.get(WEREAD_NOTEBOOKS_URL)
    if r.ok:
        data = r.json()
        books = data.get("books")
        books.sort(key=lambda x: x["sort"])
        return books
    else:
        print(r.text)
    return None


def get_sort():
    """获取database中的最新时间"""
    filter = {"property": "Sort", "number": {"is_not_empty": True}}
    sorts = [
        {
            "property": "Sort",
            "direction": "descending",
        }
    ]
    response = client.databases.query(
        database_id=database_id, filter=filter, sorts=sorts, page_size=1
    )
    if len(response.get("results")) == 1:
        return response.get("results")[0].get("properties").get("Sort").get("number")
    return 0


def get_children(chapter, summary, bookmark_list):
    children = []
    grandchild = {}
    if chapter != None:
        # 添加目录
        children.append(get_table_of_contents())
        d = {}
        for data in bookmark_list:
            chapterUid = data.get("chapterUid", 1)
            if chapterUid not in d:
                d[chapterUid] = []
            d[chapterUid].append(data)
        for key, value in d.items():
            if key in chapter:
                # 添加章节
                children.append(
                    get_heading(
                        chapter.get(key).get("level"), chapter.get(key).get("title")
                    )
                )
            for i in value:
                markText = i.get("markText")
                for j in range(0, len(markText) // 2000 + 1):
                    children.append(
                        get_callout(
                            markText[j * 2000 : (j + 1) * 2000],
                            i.get("style"),
                            i.get("colorStyle"),
                            i.get("reviewId"),
                        )
                    )
                if i.get("abstract") != None and i.get("abstract") != "":
                    quote = get_quote(i.get("abstract"))
                    grandchild[len(children) - 1] = quote

    else:
        # 如果没有章节信息
        for data in bookmark_list:
            markText = data.get("markText")
            for i in range(0, len(markText) // 2000 + 1):
                children.append(
                    get_callout(
                        markText[i * 2000 : (i + 1) * 2000],
                        data.get("style"),
                        data.get("colorStyle"),
                        data.get("reviewId"),
                    )
                )
    if summary != None and len(summary) > 0:
        children.append(get_heading(1, "点评"))
        for i in summary:
            content = i.get("review").get("content")
            for j in range(0, len(content) // 2000 + 1):
                children.append(
                    get_callout(
                        content[j * 2000 : (j + 1) * 2000],
                        i.get("style"),
                        i.get("colorStyle"),
                        i.get("review").get("reviewId"),
                    )
                )
    return children, grandchild


def transform_id(book_id):
    id_length = len(book_id)

    if re.match("^\d*$", book_id):
        ary = []
        for i in range(0, id_length, 9):
            ary.append(format(int(book_id[i : min(i + 9, id_length)]), "x"))
        return "3", ary

    result = ""
    for i in range(id_length):
        result += format(ord(book_id[i]), "x")
    return "4", [result]


def calculate_book_str_id(book_id):
    md5 = hashlib.md5()
    md5.update(book_id.encode("utf-8"))
    digest = md5.hexdigest()
    result = digest[0:3]
    code, transformed_ids = transform_id(book_id)
    result += code + "2" + digest[-2:]

    for i in range(len(transformed_ids)):
        hex_length_str = format(len(transformed_ids[i]), "x")
        if len(hex_length_str) == 1:
            hex_length_str = "0" + hex_length_str

        result += hex_length_str + transformed_ids[i]

        if i < len(transformed_ids) - 1:
            result += "g"

    if len(result) < 20:
        result += digest[0 : 20 - len(result)]

    md5 = hashlib.md5()
    md5.update(result.encode("utf-8"))
    result += md5.hexdigest()[0:3]
    return result


def download_image(url, save_dir="cover"):
    # 确保目录存在，如果不存在则创建
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # 获取文件名，使用 URL 最后一个 '/' 之后的字符串
    file_name = url.split("/")[-1] + ".jpg"
    save_path = os.path.join(save_dir, file_name)

    # 检查文件是否已经存在，如果存在则不进行下载
    if os.path.exists(save_path):
        print(f"File {file_name} already exists. Skipping download.")
        return save_path

    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=128):
                file.write(chunk)
        print(f"Image downloaded successfully to {save_path}")
    else:
        print(f"Failed to download image. Status code: {response.status_code}")
    return save_path


async def download_image_async(url, save_dir="cover"):
    """基于线程池异步下载图片"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, download_image, url, save_dir)


def try_get_cloud_cookie(url, id, password):
    if url.endswith("/"):
        url = url[:-1]
    req_url = f"{url}/get/{id}"
    data = {"password": password}
    result = None
    response = requests.post(req_url, data=data)
    if response.status_code == 200:
        data = response.json()
        cookie_data = data.get("cookie_data")
        if cookie_data and "weread.qq.com" in cookie_data:
            cookies = cookie_data["weread.qq.com"]
            cookie_str = "; ".join(
                [f"{cookie['name']}={cookie['value']}" for cookie in cookies]
            )
            result = cookie_str
    return result


def get_cookie():
    url = os.getenv("CC_URL")
    if not url:
        url = "https://cookiecloud.malinkang.com/"
    id = os.getenv("CC_ID")
    password = os.getenv("CC_PASSWORD")
    cookie = os.getenv("WEREAD_COOKIE")
    if url and id and password:
        cookie = try_get_cloud_cookie(url, id, password)
    if not cookie or not cookie.strip():
        raise Exception("没有找到cookie，请按照文档填写cookie")
    return cookie
    


def extract_page_id():
    url = os.getenv("NOTION_PAGE")
    if not url:
        url = os.getenv("NOTION_DATABASE_ID")
    if not url:
        raise Exception("没有找到NOTION_PAGE，请按照文档填写")
    # 正则表达式匹配 32 个字符的 Notion page_id
    match = re.search(
        r"([a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",
        url,
    )
    if match:
        return match.group(0)
    else:
        raise Exception(f"获取NotionID失败，请检查输入的Url是否正确")


def retry_async(
    retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0
):
    """Retry decorator for async functions"""
    def decorator(func):
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


class WeReadClient:
    """WeRead API client with async support and performance optimizations"""
    def __init__(
        self,
        session: aiohttp.ClientSession,
        notion_client: AsyncClient,
        cache: Cache,
        rate_limiter: RateLimiter,
        max_concurrent_requests: int = 10,
        request_timeout: int = 30
    ):
        self.session = session
        self.notion_client = notion_client
        self.cache = cache
        self.rate_limiter = rate_limiter
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.timeout = aiohttp.ClientTimeout(total=request_timeout)
        self.performance_monitor = PerformanceMonitor()
        
    @retry_async(retries=3)
    async def make_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> Optional[Dict]:
        """Make HTTP request with retry, rate limiting and monitoring"""
        start_time = time.monotonic()
        
        try:
            async with self.semaphore:
                await self.rate_limiter.acquire()
                async with self.session.request(
                    method,
                    url,
                    timeout=self.timeout,
                    **kwargs
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Track request performance
                    duration = time.monotonic() - start_time
                    await self.performance_monitor.track(
                        f"{method}_{url}",
                        duration
                    )
                    
                    return data
        except Exception as e:
            print(f"Request error: {e}")
            raise

    async def get_bookmark_list(
        self, 
        book_id: str,
        use_cache: bool = True
    ) -> Optional[List[Dict]]:
        """Fetch bookmark list with caching"""
        cache_key = f"bookmarks:{book_id}"
        
        if use_cache:
            cached = await self.cache.get(cache_key)
            if cached:
                return cached
                
        params = {'bookId': book_id}
        data = await self.make_request(
            'GET',
            WEREAD_BOOKMARKLIST_URL,
            params=params
        )
        
        if data:
            updated = data.get('updated', [])
            result = sorted(
                updated,
                key=lambda x: (
                    x.get('chapterUid', 1),
                    int(x.get('range', '0-0').split('-')[0])
                )
            )
            
            if use_cache:
                await self.cache.set(cache_key, result)
            return result
        return None

    async def process_books_parallel(
        self,
        books: List[Dict],
        database_id: str,
        latest_sort: int,
        batch_size: int = 5
    ):
        """Process multiple books in parallel with batching"""
        total_books = len(books)
        processed = 0
        
        for i in range(0, total_books, batch_size):
            batch = books[i:i + batch_size]
            results = await process_book_batch(
                self,
                batch,
                database_id,
                latest_sort,
                batch_size
            )
            
            processed += len(batch)
            print(f"Processed {processed}/{total_books} books")
            
            # Small delay between batches to prevent overload
            if i + batch_size < total_books:
                await asyncio.sleep(1)

    async def get_book_info(
        self,
        book_id: str,
        use_cache: bool = True
    ) -> Tuple[str, float]:
        """Fetch book info with caching"""
        cache_key = f"book_info:{book_id}"
        
        if use_cache:
            cached = await self.cache.get(cache_key)
            if cached:
                return cached
                
        params = {'bookId': book_id}
        data = await self.make_request(
            'GET',
            WEREAD_BOOK_INFO,
            params=params
        )
        
        if data:
            result = (
                data.get('isbn', ''),
                data.get('newRating', 0) / 1000
            )
            if use_cache:
                await self.cache.set(cache_key, result)
            return result
        return '', 0

async def async_check(bookId, client, database_id):
    """异步检查并删除已插入的记录，避免阻塞"""
    await asyncio.sleep(0.3)
    filter = {"property": "BookId", "rich_text": {"equals": bookId}}
    response = await client.databases.query(database_id=database_id, filter=filter)
    for result in response["results"]:
        await asyncio.sleep(0.3)
        await client.blocks.delete(block_id=result["id"])

async def main():
    """Enhanced main async function with better error handling and monitoring"""
    load_dotenv()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize components
        cookie = get_cookie()
        database_id = extract_page_id()
        notion_token = os.getenv('NOTION_TOKEN')
        redis_url = os.getenv('REDIS_URL')
        
        # Initialize cache and rate limiter
        cache = Cache(redis_url=redis_url)
        rate_limiter = RateLimiter(rate=5, capacity=10)
        
        # Connection pool settings
        conn = aiohttp.TCPConnector(
            limit=10,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        async with aiohttp.ClientSession(
            cookies=parse_cookie_string(cookie),
            connector=conn,
            timeout=aiohttp.ClientTimeout(total=300)
        ) as session:
            notion_client = AsyncClient(auth=notion_token)
            client = WeReadClient(
                session=session,
                notion_client=notion_client,
                cache=cache,
                rate_limiter=rate_limiter
            )
            
            # Get latest sort and book list
            latest_sort = await client.get_sort(database_id)
            books = await client.get_notebook_list()
            
            if not books:
                logger.error("Failed to fetch books")
                return
                
            # Process books in parallel batches
            await client.process_books_parallel(
                books=books,
                database_id=database_id,
                latest_sort=latest_sort
            )
            
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
    finally:
        # Cleanup
        if 'cache' in locals():
            await cache.clear()
            
if __name__ == '__main__':
    asyncio.run(main())
