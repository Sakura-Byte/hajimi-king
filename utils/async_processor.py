"""
异步任务处理器模块
实现GitHub搜索和key验证的并发处理
"""

import asyncio
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiohttp
import google.generativeai as genai
from google.api_core import exceptions as google_exceptions

from common.config import Config
from common.Logger import logger
from utils.file_manager import checkpoint, file_manager
from utils.sync_utils import sync_utils


@dataclass
class FileTask:
    """文件处理任务"""

    item: dict[str, Any]
    query: str
    priority: int = 0


@dataclass
class KeyValidationTask:
    """Key验证任务"""

    key: str
    repo_name: str
    file_path: str
    file_url: str
    source_task: FileTask
    priority: int = 0


@dataclass
class ProcessingStats:
    """处理统计信息"""

    github_searches: int = 0
    files_downloaded: int = 0
    keys_extracted: int = 0
    keys_validated: int = 0
    valid_keys: int = 0
    rate_limited_keys: int = 0
    errors: int = 0
    start_time: datetime | None = None

    def reset(self):
        self.github_searches = 0
        self.files_downloaded = 0
        self.keys_extracted = 0
        self.keys_validated = 0
        self.valid_keys = 0
        self.rate_limited_keys = 0
        self.errors = 0
        self.start_time = datetime.now()


class AsyncProcessor:
    """异步任务处理器"""

    def __init__(
        self,
        max_file_workers: int = 8,
        max_validation_workers: int = 5,
        file_queue_size: int = 100,
        key_queue_size: int = 50,
    ):
        # 队列配置
        self.max_file_workers = max_file_workers
        self.max_validation_workers = max_validation_workers
        self.file_queue_size = file_queue_size
        self.key_queue_size = key_queue_size

        # 异步队列
        self.file_queue: asyncio.Queue | None = None
        self.key_queue: asyncio.Queue | None = None

        # Worker任务列表
        self.file_workers: list[asyncio.Task] = []
        self.validation_workers: list[asyncio.Task] = []

        # 统计信息
        self.stats = ProcessingStats()

        # 控制标志
        self.shutdown_event: asyncio.Event | None = None
        self.is_running = False

        # GitHub客户端相关
        self.github_tokens = Config.GITHUB_TOKENS.copy()
        self.token_ptr = 0

        logger.info(
            f"🚀 AsyncProcessor initialized - File workers: {max_file_workers}, Validation workers: {max_validation_workers}"
        )

    async def start(self):
        """启动异步处理器"""
        if self.is_running:
            logger.warning("AsyncProcessor is already running")
            return

        logger.info("🚀 Starting AsyncProcessor...")

        # 创建队列
        self.file_queue = asyncio.Queue(maxsize=self.file_queue_size)
        self.key_queue = asyncio.Queue(maxsize=self.key_queue_size)

        # 创建shutdown事件
        self.shutdown_event = asyncio.Event()

        # 启动文件处理workers
        self.file_workers = []
        for i in range(self.max_file_workers):
            worker = asyncio.create_task(self._file_worker(f"file-worker-{i}"))
            self.file_workers.append(worker)

        # 启动key验证workers
        self.validation_workers = []
        for i in range(self.max_validation_workers):
            worker = asyncio.create_task(self._validation_worker(f"validation-worker-{i}"))
            self.validation_workers.append(worker)

        self.is_running = True
        self.stats.reset()

        logger.info(
            f"✅ AsyncProcessor started with {len(self.file_workers)} file workers and {len(self.validation_workers)} validation workers"
        )

    async def stop(self):
        """停止异步处理器"""
        if not self.is_running:
            return

        logger.info("🛑 Stopping AsyncProcessor...")

        # 设置shutdown事件
        if self.shutdown_event:
            self.shutdown_event.set()

        # 等待所有worker完成
        all_workers = self.file_workers + self.validation_workers
        if all_workers:
            await asyncio.gather(*all_workers, return_exceptions=True)

        self.is_running = False
        logger.info("✅ AsyncProcessor stopped")

    async def add_file_task(self, item: dict[str, Any], query: str) -> bool:
        """添加文件处理任务到队列"""
        if not self.is_running or not self.file_queue:
            return False

        task = FileTask(item=item, query=query)

        try:
            await self.file_queue.put(task)
            return True
        except asyncio.QueueFull:
            logger.warning("📦 File queue is full, task dropped")
            return False

    async def _file_worker(self, worker_name: str):
        """文件处理worker"""
        logger.info(f"👷 {worker_name} started")

        while not self.shutdown_event.is_set():
            try:
                # 等待任务，设置超时避免死锁
                task = await asyncio.wait_for(self.file_queue.get(), timeout=1.0)

                await self._process_file_task(task, worker_name)
                self.file_queue.task_done()

            except TimeoutError:
                continue
            except Exception as e:
                logger.error(f"❌ {worker_name} error: {e}")
                self.stats.errors += 1

        logger.info(f"👷 {worker_name} stopped")

    async def _validation_worker(self, worker_name: str):
        """Key验证worker"""
        logger.info(f"🔐 {worker_name} started")

        while not self.shutdown_event.is_set():
            try:
                # 等待任务，设置超时避免死锁
                task = await asyncio.wait_for(self.key_queue.get(), timeout=1.0)

                await self._process_validation_task(task, worker_name)
                self.key_queue.task_done()

            except TimeoutError:
                continue
            except Exception as e:
                logger.error(f"❌ {worker_name} error: {e}")
                self.stats.errors += 1

        logger.info(f"🔐 {worker_name} stopped")

    async def _process_file_task(self, task: FileTask, worker_name: str):
        """处理单个文件任务"""
        item = task.item

        # 检查是否应该跳过
        should_skip, skip_reason = self._should_skip_item(item)
        if should_skip:
            logger.debug(f"🚫 {worker_name} skipping {item.get('path', '')} - {skip_reason}")
            return

        # 下载文件内容
        content = await self._download_file_content(item, worker_name)
        if not content:
            return

        self.stats.files_downloaded += 1

        # 提取keys
        keys = self._extract_keys_from_content(content)
        if not keys:
            return

        self.stats.keys_extracted += len(keys)
        logger.info(f"🔑 {worker_name} found {len(keys)} keys in {item.get('path', '')}")

        # 创建验证任务
        repo_name = item["repository"]["full_name"]
        file_path = item["path"]
        file_url = item["html_url"]

        for key in keys:
            validation_task = KeyValidationTask(
                key=key, repo_name=repo_name, file_path=file_path, file_url=file_url, source_task=task
            )

            try:
                await self.key_queue.put(validation_task)
            except asyncio.QueueFull:
                logger.warning(f"📦 Key validation queue full, dropping key: {key[:20]}...")

    async def _process_validation_task(self, task: KeyValidationTask, worker_name: str):
        """处理key验证任务"""
        validation_result = await self._validate_gemini_key_async(task.key, worker_name)

        self.stats.keys_validated += 1

        if validation_result and "ok" in validation_result:
            # 有效key
            self.stats.valid_keys += 1
            logger.info(f"✅ {worker_name} VALID: {task.key}")

            # 保存到文件
            file_manager.save_valid_keys(task.repo_name, task.file_path, task.file_url, [task.key])

            # 添加到同步队列
            try:
                sync_utils.add_keys_to_queue([task.key])
                logger.debug(f"📥 {worker_name} added key to sync queue")
            except Exception as e:
                logger.error(f"📥 {worker_name} sync queue error: {e}")

        elif validation_result == "rate_limited":
            # 被限流的key
            self.stats.rate_limited_keys += 1
            logger.warning(f"⚠️ {worker_name} RATE LIMITED: {task.key}")

            file_manager.save_rate_limited_keys(task.repo_name, task.file_path, task.file_url, [task.key])
        else:
            # 无效key
            logger.debug(f"❌ {worker_name} INVALID: {task.key} - {validation_result}")

    async def _download_file_content(self, item: dict[str, Any], worker_name: str) -> str | None:
        """异步下载文件内容"""
        repo_full_name = item["repository"]["full_name"]
        file_path = item["path"]
        metadata_url = f"https://api.github.com/repos/{repo_full_name}/contents/{file_path}"

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        }

        # 获取token
        token = self._get_next_token()
        if token:
            headers["Authorization"] = f"token {token}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(metadata_url, headers=headers) as response:
                    response.raise_for_status()
                    file_metadata = await response.json()

                    # 尝试base64解码
                    encoding = file_metadata.get("encoding")
                    content = file_metadata.get("content")

                    if encoding == "base64" and content:
                        import base64

                        try:
                            decoded_content = base64.b64decode(content).decode("utf-8")
                            return decoded_content
                        except Exception:
                            pass

                    # 使用download_url
                    download_url = file_metadata.get("download_url")
                    if not download_url:
                        return None

                    async with session.get(download_url, headers=headers) as content_response:
                        content_response.raise_for_status()
                        return await content_response.text()

        except Exception as e:
            logger.error(f"❌ {worker_name} download failed: {metadata_url} - {type(e).__name__}")
            return None

    async def _validate_gemini_key_async(self, api_key: str, worker_name: str) -> str:
        """异步验证Gemini API key"""
        try:
            # 随机延迟避免rate limit
            await asyncio.sleep(random.uniform(0.5, 1.5))

            # 在新的线程中执行同步的Gemini API调用
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._validate_gemini_key_sync, api_key)
            return result

        except Exception as e:
            logger.error(f"❌ {worker_name} validation error: {e}")
            return f"error:{e.__class__.__name__}"

    def _validate_gemini_key_sync(self, api_key: str) -> str:
        """同步验证Gemini API key (在executor中运行)"""
        try:
            # 配置代理
            proxy_config = Config.get_random_proxy()
            if proxy_config:
                import os

                os.environ["grpc_proxy"] = proxy_config.get("http")

            # 配置Gemini客户端
            genai.configure(api_key=api_key, client_options={"api_endpoint": "generativelanguage.googleapis.com"})

            # 发送测试请求
            model = genai.GenerativeModel(Config.HAJIMI_CHECK_MODEL)
            model.generate_content("hi")
            return "ok"

        except (google_exceptions.PermissionDenied, google_exceptions.Unauthenticated):
            return "not_authorized_key"
        except google_exceptions.TooManyRequests:
            return "rate_limited"
        except Exception as e:
            if "429" in str(e) or "rate limit" in str(e).lower() or "quota" in str(e).lower():
                return "rate_limited:429"
            if "403" in str(e) or "SERVICE_DISABLED" in str(e) or "API has not been used" in str(e):
                return "disabled"
            return f"error:{e.__class__.__name__}"

    def _should_skip_item(self, item: dict[str, Any]) -> tuple[bool, str]:
        """检查是否应该跳过处理此item (从原代码移植)"""
        # 检查增量扫描时间
        if checkpoint.last_scan_time:
            try:
                from datetime import datetime, timedelta

                last_scan_dt = datetime.fromisoformat(checkpoint.last_scan_time)
                repo_pushed_at = item["repository"].get("pushed_at")
                if repo_pushed_at:
                    repo_pushed_dt = datetime.strptime(repo_pushed_at, "%Y-%m-%dT%H:%M:%SZ")
                    if repo_pushed_dt <= last_scan_dt:
                        return True, "time_filter"
            except Exception:
                pass

        # 检查SHA是否已扫描
        if item.get("sha") in checkpoint.scanned_shas:
            return True, "sha_duplicate"

        # 检查仓库年龄
        repo_pushed_at = item["repository"].get("pushed_at")
        if repo_pushed_at:
            from datetime import datetime, timedelta

            repo_pushed_dt = datetime.strptime(repo_pushed_at, "%Y-%m-%dT%H:%M:%SZ")
            if repo_pushed_dt < datetime.utcnow() - timedelta(days=Config.DATE_RANGE_DAYS):
                return True, "age_filter"

        # 检查文档和示例文件
        lowercase_path = item["path"].lower()
        if any(token in lowercase_path for token in Config.FILE_PATH_BLACKLIST):
            return True, "doc_filter"

        return False, ""

    def _extract_keys_from_content(self, content: str) -> list[str]:
        """从文件内容中提取API keys (从原代码移植)"""
        import re

        pattern = r"(AIzaSy[A-Za-z0-9\-_]{33})"
        keys = re.findall(pattern, content)

        # 过滤占位符密钥
        filtered_keys = []
        for key in keys:
            context_index = content.find(key)
            if context_index != -1:
                snippet = content[context_index : context_index + 45]
                if "..." in snippet or "YOUR_" in snippet.upper():
                    continue
            filtered_keys.append(key)

        return list(set(filtered_keys))  # 去重

    def _get_next_token(self) -> str | None:
        """获取下一个GitHub token"""
        if not self.github_tokens:
            return None

        token = self.github_tokens[self.token_ptr % len(self.github_tokens)]
        self.token_ptr += 1
        return token.strip() if isinstance(token, str) else token

    def get_stats(self) -> ProcessingStats:
        """获取处理统计信息"""
        return self.stats

    def get_queue_status(self) -> dict[str, Any]:
        """获取队列状态"""
        return {
            "file_queue_size": self.file_queue.qsize() if self.file_queue else 0,
            "key_queue_size": self.key_queue.qsize() if self.key_queue else 0,
            "file_workers": len(self.file_workers),
            "validation_workers": len(self.validation_workers),
            "is_running": self.is_running,
        }
