"""
重构的异步处理器 - 集成所有并发优化
基于API rate limit的智能并发模型
"""

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiohttp
import google.generativeai as genai
from google.api_core import exceptions as google_exceptions

from common.config import Config
from common.Logger import logger
from utils.file_manager import file_manager
from utils.sync_utils import sync_utils

from .rate_limit.adaptive_scheduler import AdaptiveScheduler, TaskType
from .rate_limit.backpressure_rl import RateLimitBackpressure
from .rate_limit.priority_queue import FileTaskInfo, KeyValidationTaskInfo, PriorityCalculator, QuotaAwarePriorityQueue

# 导入新的并发优化组件
from .rate_limit.quota_monitor import QuotaMonitor
from .rate_limit.smart_load_balancer import PredictiveLoadBalancer
from .rate_limit.token_manager import TokenManager
from .rate_limit.work_stealing import WorkStealingScheduler


@dataclass
class OptimizedProcessingStats:
    """优化后的处理统计信息"""

    github_searches: int = 0
    files_downloaded: int = 0
    keys_extracted: int = 0
    keys_validated: int = 0
    valid_keys: int = 0
    rate_limited_keys: int = 0
    errors: int = 0
    start_time: datetime | None = None

    # 新增优化指标
    avg_queue_wait_time: float = 0.0
    worker_utilization: float = 0.0
    api_quota_efficiency: float = 0.0
    backpressure_activations: int = 0
    work_steals_successful: int = 0
    load_rebalances: int = 0

    def reset(self):
        """重置所有统计"""
        for field in self.__dataclass_fields__:
            if field != "start_time":
                setattr(self, field, 0 if not field.startswith("avg_") else 0.0)
        self.start_time = datetime.now()


class OptimizedAsyncProcessor:
    """优化的异步任务处理器"""

    def __init__(
        self,
        max_file_workers: int = 8,
        max_validation_workers: int = 5,
        file_queue_size: int = 200,
        key_queue_size: int = 100,
        enable_work_stealing: bool = True,
        enable_smart_load_balancing: bool = True,
    ):
        # 基础配置
        self.max_file_workers = max_file_workers
        self.max_validation_workers = max_validation_workers
        self.enable_work_stealing = enable_work_stealing
        self.enable_smart_load_balancing = enable_smart_load_balancing

        # 核心组件初始化
        self.quota_monitor = QuotaMonitor()
        self.token_manager = TokenManager(self.quota_monitor)
        self.backpressure = RateLimitBackpressure(self.quota_monitor, self.token_manager)
        self.adaptive_scheduler = AdaptiveScheduler(self.quota_monitor, self.token_manager, self.backpressure)

        # 优先级队列
        self.file_priority_queue = QuotaAwarePriorityQueue("github", file_queue_size)
        self.key_priority_queue = QuotaAwarePriorityQueue("gemini", key_queue_size)

        # 工作窃取调度器（可选）
        if enable_work_stealing:
            self.work_stealing_scheduler = WorkStealingScheduler(self.quota_monitor, self.token_manager)
        else:
            self.work_stealing_scheduler = None

        # 智能负载均衡器（可选）
        if enable_smart_load_balancing:
            self.load_balancer = PredictiveLoadBalancer(self.quota_monitor, self.token_manager)
        else:
            self.load_balancer = None

        # Worker管理
        self.file_workers: list[asyncio.Task] = []
        self.validation_workers: list[asyncio.Task] = []
        self.active_workers = {"file": 0, "validation": 0}

        # 控制和状态
        self.shutdown_event: asyncio.Event | None = None
        self.is_running = False
        self.stats = OptimizedProcessingStats()

        # 后台任务
        self.monitoring_task: asyncio.Task | None = None
        self.rebalancing_task: asyncio.Task | None = None

        logger.info("🚀 OptimizedAsyncProcessor initialized")
        logger.info(f"   📊 Max workers: {max_file_workers} file, {max_validation_workers} validation")
        logger.info(f"   🎯 Work stealing: {enable_work_stealing}")
        logger.info(f"   ⚖️ Smart load balancing: {enable_smart_load_balancing}")

    async def start(self):
        """启动优化的异步处理器"""
        if self.is_running:
            logger.warning("OptimizedAsyncProcessor is already running")
            return

        logger.info("🚀 Starting OptimizedAsyncProcessor...")

        # 创建shutdown事件
        self.shutdown_event = asyncio.Event()

        # 启动初始worker
        await self._start_initial_workers()

        # 启动后台监控任务
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())

        # 启动负载重新均衡任务（如果启用）
        if self.load_balancer:
            self.rebalancing_task = asyncio.create_task(self._rebalancing_loop())

        self.is_running = True
        self.stats.reset()

        logger.info("✅ OptimizedAsyncProcessor started")

    async def stop(self):
        """停止异步处理器"""
        if not self.is_running:
            return

        logger.info("🛑 Stopping OptimizedAsyncProcessor...")

        # 设置shutdown事件
        if self.shutdown_event:
            self.shutdown_event.set()

        # 停止后台任务
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass

        if self.rebalancing_task:
            self.rebalancing_task.cancel()
            try:
                await self.rebalancing_task
            except asyncio.CancelledError:
                pass

        # 等待所有worker完成
        all_workers = self.file_workers + self.validation_workers
        if all_workers:
            await asyncio.gather(*all_workers, return_exceptions=True)

        self.is_running = False
        logger.info("✅ OptimizedAsyncProcessor stopped")

    async def add_file_task(self, item: dict[str, Any], query: str, priority: int | None = None) -> bool:
        """添加文件处理任务"""
        if not self.is_running:
            return False

        # 构建任务信息
        task_info = FileTaskInfo(
            repo_name=item["repository"]["full_name"],
            file_path=item["path"],
            file_size=item.get("size", 1024),  # 默认1KB
            repo_stars=item["repository"].get("stargazers_count", 0),
            repo_updated_at=item["repository"].get("pushed_at", ""),
            file_url=item["html_url"],
        )

        # 计算优先级（如果未指定）
        if priority is None:
            quota_status = self.quota_monitor.get_quota_summary()
            calculator = PriorityCalculator()
            priority = calculator.calculate_file_task_priority(task_info, quota_status)

        # 添加到优先级队列
        success = self.file_priority_queue.add_file_task(
            task_id=f"file_{item.get('sha', 'unknown')}",
            task_info=task_info,
            current_quota_status=self.quota_monitor.get_quota_summary(),
            task_data={"item": item, "query": query},
        )

        if success:
            logger.debug(f"📥 Added file task for {task_info.file_path} with priority {priority}")

        return success

    async def _start_initial_workers(self):
        """启动初始worker"""
        # 启动文件处理worker
        initial_file_workers = min(self.max_file_workers, len(self.quota_monitor.github_tokens) * 2)
        for i in range(initial_file_workers):
            await self._start_file_worker(f"file-worker-{i}")

        # 启动验证worker
        for i in range(self.max_validation_workers):
            await self._start_validation_worker(f"validation-worker-{i}")

    async def _start_file_worker(self, worker_id: str):
        """启动文件处理worker"""
        if self.work_stealing_scheduler:
            # 使用工作窃取调度器
            self.work_stealing_scheduler.register_worker(worker_id, TaskType.GITHUB_FILE)

        worker_task = asyncio.create_task(self._file_worker_with_optimizations(worker_id))
        self.file_workers.append(worker_task)
        self.active_workers["file"] += 1

        logger.debug(f"👷 Started file worker: {worker_id}")

    async def _start_validation_worker(self, worker_id: str):
        """启动验证worker"""
        if self.work_stealing_scheduler:
            self.work_stealing_scheduler.register_worker(worker_id, TaskType.GEMINI_VALIDATION)

        worker_task = asyncio.create_task(self._validation_worker_with_optimizations(worker_id))
        self.validation_workers.append(worker_task)
        self.active_workers["validation"] += 1

        logger.debug(f"🔐 Started validation worker: {worker_id}")

    async def _file_worker_with_optimizations(self, worker_id: str):
        """优化的文件处理worker"""
        logger.info(f"👷 {worker_id} started with optimizations")

        while not self.shutdown_event.is_set():
            try:
                # 检查背压状态
                if self.backpressure.should_pause_github_tasks():
                    await asyncio.sleep(5)
                    continue

                # 获取任务
                task_data = None
                if self.work_stealing_scheduler:
                    # 使用工作窃取调度器
                    scheduled_task = self.work_stealing_scheduler.get_next_task_for_worker(worker_id)
                    if scheduled_task:
                        task_data = scheduled_task.payload
                else:
                    # 使用优先级队列
                    task_result = self.file_priority_queue.get_next_task()
                    if task_result:
                        task_id, task_data = task_result

                if not task_data:
                    await asyncio.sleep(1)
                    continue

                # 应用背压延迟
                delay = await self.backpressure.get_delay_for_github_task()
                if delay > 0:
                    await asyncio.sleep(delay)

                # 处理任务
                start_time = time.time()
                await self._process_file_task_optimized(task_data, worker_id)
                time.time() - start_time

                # 记录完成
                if self.work_stealing_scheduler:
                    self.work_stealing_scheduler.mark_task_completed(worker_id, scheduled_task)

                self.stats.files_downloaded += 1

            except Exception as e:
                logger.error(f"❌ {worker_id} error: {e}")
                self.stats.errors += 1
                await asyncio.sleep(2)  # 错误后短暂休息

        logger.info(f"👷 {worker_id} stopped")

    async def _validation_worker_with_optimizations(self, worker_id: str):
        """优化的验证worker"""
        logger.info(f"🔐 {worker_id} started with optimizations")

        while not self.shutdown_event.is_set():
            try:
                # 检查背压状态
                if self.backpressure.should_pause_gemini_tasks():
                    await asyncio.sleep(5)
                    continue

                # 获取任务
                task_data = None
                if self.work_stealing_scheduler:
                    scheduled_task = self.work_stealing_scheduler.get_next_task_for_worker(worker_id)
                    if scheduled_task:
                        task_data = scheduled_task.payload
                else:
                    task_result = self.key_priority_queue.get_next_task()
                    if task_result:
                        task_id, task_data = task_result

                if not task_data:
                    await asyncio.sleep(1)
                    continue

                # 应用背压延迟
                delay = await self.backpressure.get_delay_for_gemini_task()
                if delay > 0:
                    await asyncio.sleep(delay)

                # 处理任务
                start_time = time.time()
                await self._process_validation_task_optimized(task_data, worker_id)
                time.time() - start_time

                # 记录完成
                if self.work_stealing_scheduler:
                    self.work_stealing_scheduler.mark_task_completed(worker_id, scheduled_task)

                self.stats.keys_validated += 1

            except Exception as e:
                logger.error(f"❌ {worker_id} error: {e}")
                self.stats.errors += 1
                await asyncio.sleep(2)

        logger.info(f"🔐 {worker_id} stopped")

    async def _process_file_task_optimized(self, task_data: dict, worker_id: str):
        """优化的文件任务处理"""
        item = task_data["item"]
        task_data["query"]

        # 检查是否应该跳过
        if self._should_skip_item(item):
            return

        # 获取最佳token（使用负载均衡器）
        token_decision = None
        if self.load_balancer:
            token_decision = await self.load_balancer.get_best_github_token(task_priority=5)

        if not token_decision:
            # Fallback到token manager
            token = self.token_manager.get_best_github_token(task_priority=5)
        else:
            token = token_decision.resource_id

        if not token:
            logger.warning(f"🚫 {worker_id} no available token")
            return

        # 下载文件内容
        start_time = time.time()
        content = await self._download_file_content_optimized(item, token, worker_id)
        response_time = time.time() - start_time

        # 记录token性能
        success = content is not None
        self.token_manager.record_token_usage(token, success, response_time)

        if self.load_balancer and token_decision:
            self.load_balancer.record_resource_performance(token, success, response_time)

        if not content:
            return

        # 提取keys
        keys = self._extract_keys_from_content(content)
        if not keys:
            return

        self.stats.keys_extracted += len(keys)

        # 为每个key创建验证任务
        repo_name = item["repository"]["full_name"]
        file_path = item["path"]
        repo_stars = item["repository"].get("stargazers_count", 0)

        for key in keys:
            # 创建验证任务信息
            task_info = KeyValidationTaskInfo(
                key=key,
                repo_name=repo_name,
                file_path=file_path,
                repo_stars=repo_stars,
                key_source_context=self._get_key_context(content, key),
            )

            # 添加到验证队列
            self.key_priority_queue.add_validation_task(
                task_id=f"key_{hash(key)}",
                task_info=task_info,
                current_quota_status=self.quota_monitor.get_quota_summary(),
                task_data={"key": key, "repo_name": repo_name, "file_path": file_path, "file_url": item["html_url"]},
            )

    async def _process_validation_task_optimized(self, task_data: dict, worker_id: str):
        """优化的验证任务处理"""
        key = task_data["key"]
        repo_name = task_data["repo_name"]
        file_path = task_data["file_path"]
        file_url = task_data["file_url"]

        # 验证key
        start_time = time.time()
        result = await self._validate_gemini_key_async(key, worker_id)
        response_time = time.time() - start_time

        # 记录Gemini性能
        success = result and "ok" in result
        is_429 = result == "rate_limited" or "429" in str(result)
        self.quota_monitor.record_gemini_call(success, response_time, is_429)

        # 处理验证结果
        if success:
            self.stats.valid_keys += 1
            logger.info(f"✅ {worker_id} VALID: {key}")

            file_manager.save_valid_keys(repo_name, file_path, file_url, [key])

            try:
                sync_utils.add_keys_to_queue([key])
            except Exception as e:
                logger.error(f"📥 {worker_id} sync queue error: {e}")

        elif result == "rate_limited":
            self.stats.rate_limited_keys += 1
            logger.warning(f"⚠️ {worker_id} RATE LIMITED: {key}")
            file_manager.save_rate_limited_keys(repo_name, file_path, file_url, [key])

    async def _download_file_content_optimized(self, item: dict[str, Any], token: str, worker_id: str) -> str | None:
        """优化的文件内容下载"""
        repo_full_name = item["repository"]["full_name"]
        file_path = item["path"]
        metadata_url = f"https://api.github.com/repos/{repo_full_name}/contents/{file_path}"

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Authorization": f"token {token}",
        }

        # 获取最佳代理
        proxy_config = None
        if self.load_balancer:
            proxy_decision = await self.load_balancer.get_best_proxy()
            if proxy_decision:
                proxy_url = Config.PROXY_LIST[0] if Config.PROXY_LIST else None  # 简化实现
                if proxy_url:
                    proxy_config = {"http": proxy_url, "https": proxy_url}

        if not proxy_config:
            proxy_config = self.token_manager.get_best_proxy()

        try:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector()

            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.get(
                    metadata_url, headers=headers, proxy=proxy_config.get("http") if proxy_config else None
                ) as response:
                    # 更新配额状态
                    await self.quota_monitor.update_github_quota(token, dict(response.headers))

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

                    async with session.get(
                        download_url, headers=headers, proxy=proxy_config.get("http") if proxy_config else None
                    ) as content_response:
                        content_response.raise_for_status()
                        return await content_response.text()

        except Exception as e:
            logger.error(f"❌ {worker_id} download failed: {metadata_url} - {type(e).__name__}")
            return None

    async def _validate_gemini_key_async(self, api_key: str, worker_id: str) -> str:
        """异步验证Gemini API key"""
        try:
            # 随机延迟避免rate limit
            await asyncio.sleep(random.uniform(0.5, 1.5))

            # 在新的线程中执行同步的Gemini API调用
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._validate_gemini_key_sync, api_key)
            return result

        except Exception as e:
            logger.error(f"❌ {worker_id} validation error: {e}")
            return f"error:{e.__class__.__name__}"

    def _validate_gemini_key_sync(self, api_key: str) -> str:
        """同步验证Gemini API key (在executor中运行)"""
        try:
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

    async def _monitoring_loop(self):
        """后台监控循环"""
        logger.info("📊 Starting monitoring loop")

        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(30)  # 每30秒监控一次

                # 更新worker数量限制
                await self.adaptive_scheduler.update_worker_limits()

                # 检查是否需要调整worker数量
                await self._adjust_worker_count()

                # 清理过期任务
                self.file_priority_queue.clear_low_priority_tasks()
                self.key_priority_queue.clear_low_priority_tasks()

                # 重新评估任务优先级
                if random.random() < 0.1:  # 10%概率重新评估
                    current_quota = self.quota_monitor.get_quota_summary()
                    self.file_priority_queue.reprioritize_tasks(current_quota)
                    self.key_priority_queue.reprioritize_tasks(current_quota)

            except Exception as e:
                logger.error(f"❌ Monitoring loop error: {e}")
                await asyncio.sleep(10)

    async def _rebalancing_loop(self):
        """负载重新均衡循环"""
        if not self.load_balancer:
            return

        logger.info("⚖️ Starting load rebalancing loop")

        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(120)  # 每2分钟重新均衡一次

                rebalance_result = self.load_balancer.rebalance_load()
                if rebalance_result["actions_taken"] > 0:
                    self.stats.load_rebalances += 1
                    logger.info(f"⚖️ Load rebalanced with {rebalance_result['actions_taken']} actions")

            except Exception as e:
                logger.error(f"❌ Rebalancing loop error: {e}")
                await asyncio.sleep(30)

    async def _adjust_worker_count(self):
        """动态调整worker数量"""
        # 获取推荐的worker数量
        recommended_github = self.quota_monitor.get_recommended_github_workers()
        recommended_gemini = self.quota_monitor.get_recommended_gemini_workers()

        # 调整文件worker数量
        current_file_workers = len(self.file_workers)
        if recommended_github > current_file_workers and recommended_github <= self.max_file_workers:
            # 增加worker
            for i in range(recommended_github - current_file_workers):
                worker_id = f"file-worker-dynamic-{int(time.time())}-{i}"
                await self._start_file_worker(worker_id)
                logger.info(f"📈 Added file worker: {worker_id}")

        elif recommended_github < current_file_workers:
            # 减少worker (通过不启动新任务自然减少)
            excess_workers = current_file_workers - recommended_github
            logger.info(f"📉 Will naturally reduce {excess_workers} file workers")

        # 调整验证worker数量
        current_validation_workers = len(self.validation_workers)
        if recommended_gemini > current_validation_workers and recommended_gemini <= self.max_validation_workers:
            for i in range(recommended_gemini - current_validation_workers):
                worker_id = f"validation-worker-dynamic-{int(time.time())}-{i}"
                await self._start_validation_worker(worker_id)
                logger.info(f"📈 Added validation worker: {worker_id}")

    def _should_skip_item(self, item: dict[str, Any]) -> tuple[bool, str]:
        """检查是否应该跳过处理此item"""
        # 复用原来的逻辑
        from utils.async_processor import AsyncProcessor

        temp_processor = AsyncProcessor()
        return temp_processor._should_skip_item(item)

    def _extract_keys_from_content(self, content: str) -> list[str]:
        """从文件内容中提取API keys"""
        # 复用原来的逻辑
        from utils.async_processor import AsyncProcessor

        temp_processor = AsyncProcessor()
        return temp_processor._extract_keys_from_content(content)

    def _get_key_context(self, content: str, key: str) -> str:
        """获取key周围的上下文"""
        key_index = content.find(key)
        if key_index == -1:
            return ""

        start = max(0, key_index - 50)
        end = min(len(content), key_index + len(key) + 50)
        return content[start:end]

    def get_optimized_stats(self) -> dict:
        """获取优化后的统计信息"""
        base_stats = {
            "github_searches": self.stats.github_searches,
            "files_downloaded": self.stats.files_downloaded,
            "keys_extracted": self.stats.keys_extracted,
            "keys_validated": self.stats.keys_validated,
            "valid_keys": self.stats.valid_keys,
            "rate_limited_keys": self.stats.rate_limited_keys,
            "errors": self.stats.errors,
        }

        # 添加优化指标
        quota_summary = self.quota_monitor.get_quota_summary()
        backpressure_summary = self.backpressure.get_status_summary()

        optimized_stats = {
            **base_stats,
            "optimization_metrics": {
                "backpressure_level": backpressure_summary["level"],
                "github_health_score": quota_summary["github"]["avg_health_score"],
                "gemini_health": "good" if not quota_summary["gemini"]["recent_429"] else "degraded",
                "queue_sizes": {
                    "file_queue": self.file_priority_queue.get_queue_stats()["current_size"],
                    "key_queue": self.key_priority_queue.get_queue_stats()["current_size"],
                },
                "worker_counts": {
                    "file_workers": len(self.file_workers),
                    "validation_workers": len(self.validation_workers),
                },
            },
        }

        # 添加工作窃取统计（如果启用）
        if self.work_stealing_scheduler:
            stealing_stats = self.work_stealing_scheduler.get_scheduler_stats()
            optimized_stats["work_stealing"] = stealing_stats

        # 添加负载均衡统计（如果启用）
        if self.load_balancer:
            lb_stats = self.load_balancer.get_load_balancer_stats()
            optimized_stats["load_balancing"] = lb_stats

        return optimized_stats

    def get_queue_status(self) -> dict[str, Any]:
        """获取队列状态"""
        return {
            "file_queue": self.file_priority_queue.get_queue_stats(),
            "key_queue": self.key_priority_queue.get_queue_stats(),
            "backpressure": self.backpressure.get_status_summary(),
            "quota_summary": self.quota_monitor.get_quota_summary(),
        }
