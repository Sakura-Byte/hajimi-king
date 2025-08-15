"""
自适应任务调度器
基于API配额和系统状态智能调度任务
"""

import heapq
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from common.Logger import logger

from .backpressure_rl import RateLimitBackpressure
from .quota_monitor import QuotaMonitor
from .token_manager import TokenManager


class TaskType(Enum):
    """任务类型"""

    GITHUB_FILE = "github_file"
    GEMINI_VALIDATION = "gemini_validation"


@dataclass
class ScheduledTask:
    """调度任务"""

    task_id: str
    task_type: TaskType
    priority: int
    payload: Any
    created_time: float = field(default_factory=time.time)
    scheduled_time: float = field(default_factory=time.time)
    retry_count: int = 0
    max_retries: int = 3

    def __lt__(self, other):
        """优先级比较 (数字越小优先级越高)"""
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.scheduled_time < other.scheduled_time


class AdaptiveScheduler:
    """自适应调度器"""

    def __init__(self, quota_monitor: QuotaMonitor, token_manager: TokenManager, backpressure: RateLimitBackpressure):
        self.quota_monitor = quota_monitor
        self.token_manager = token_manager
        self.backpressure = backpressure

        # 任务队列 - 使用优先级队列
        self.github_queue: list[ScheduledTask] = []
        self.gemini_queue: list[ScheduledTask] = []

        # Worker管理
        self.active_github_workers = 0
        self.active_gemini_workers = 0
        self.max_github_workers = 8
        self.max_gemini_workers = 5

        # 调度统计
        self.stats = {
            "tasks_scheduled": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "tasks_rejected": 0,
            "github_queue_size": 0,
            "gemini_queue_size": 0,
            "avg_wait_time": 0.0,
        }

        # 任务历史 (用于学习和优化)
        self.task_history: dict[str, list[float]] = defaultdict(list)  # task_type -> [execution_times]

        logger.info("🎯 AdaptiveScheduler initialized")

    async def schedule_github_task(self, task_id: str, payload: Any, priority: int = 5) -> bool:
        """调度GitHub任务"""
        # 检查是否应该拒绝任务
        if await self._should_reject_task(TaskType.GITHUB_FILE, priority):
            self.stats["tasks_rejected"] += 1
            logger.warning(f"🚫 Rejected GitHub task {task_id} due to backpressure")
            return False

        # 计算调度时间
        delay = await self.backpressure.get_delay_for_github_task(priority)
        scheduled_time = time.time() + delay

        # 创建任务
        task = ScheduledTask(
            task_id=task_id,
            task_type=TaskType.GITHUB_FILE,
            priority=priority,
            payload=payload,
            scheduled_time=scheduled_time,
        )

        # 加入队列
        heapq.heappush(self.github_queue, task)
        self.stats["tasks_scheduled"] += 1
        self.stats["github_queue_size"] = len(self.github_queue)

        logger.debug(f"📥 Scheduled GitHub task {task_id} with priority {priority}, delay {delay:.1f}s")
        return True

    async def schedule_gemini_task(self, task_id: str, payload: Any, priority: int = 5) -> bool:
        """调度Gemini任务"""
        # 检查是否应该拒绝任务
        if await self._should_reject_task(TaskType.GEMINI_VALIDATION, priority):
            self.stats["tasks_rejected"] += 1
            logger.warning(f"🚫 Rejected Gemini task {task_id} due to backpressure")
            return False

        # 计算调度时间
        delay = await self.backpressure.get_delay_for_gemini_task(priority)
        scheduled_time = time.time() + delay

        # 创建任务
        task = ScheduledTask(
            task_id=task_id,
            task_type=TaskType.GEMINI_VALIDATION,
            priority=priority,
            payload=payload,
            scheduled_time=scheduled_time,
        )

        # 加入队列
        heapq.heappush(self.gemini_queue, task)
        self.stats["tasks_scheduled"] += 1
        self.stats["gemini_queue_size"] = len(self.gemini_queue)

        logger.debug(f"📥 Scheduled Gemini task {task_id} with priority {priority}, delay {delay:.1f}s")
        return True

    async def get_next_github_task(self) -> ScheduledTask | None:
        """获取下一个GitHub任务"""
        if not self.github_queue:
            return None

        current_time = time.time()

        # 检查队列顶部任务是否可以执行
        if self.github_queue[0].scheduled_time <= current_time:
            task = heapq.heappop(self.github_queue)
            self.stats["github_queue_size"] = len(self.github_queue)

            # 计算等待时间
            wait_time = current_time - task.created_time
            self._update_avg_wait_time(wait_time)

            return task

        return None

    async def get_next_gemini_task(self) -> ScheduledTask | None:
        """获取下一个Gemini任务"""
        if not self.gemini_queue:
            return None

        current_time = time.time()

        # 检查队列顶部任务是否可以执行
        if self.gemini_queue[0].scheduled_time <= current_time:
            task = heapq.heappop(self.gemini_queue)
            self.stats["gemini_queue_size"] = len(self.gemini_queue)

            # 计算等待时间
            wait_time = current_time - task.created_time
            self._update_avg_wait_time(wait_time)

            return task

        return None

    async def _should_reject_task(self, task_type: TaskType, priority: int) -> bool:
        """判断是否应该拒绝任务"""
        await self.backpressure.evaluate_backpressure()

        # 高优先级任务很少被拒绝
        if priority <= 2:
            return False

        # 根据背压等级决定
        if task_type == TaskType.GITHUB_FILE:
            if self.backpressure.should_pause_github_tasks():
                return True

            # 队列过长时拒绝低优先级任务
            if len(self.github_queue) > 200 and priority >= 7:
                return True

        elif task_type == TaskType.GEMINI_VALIDATION:
            if self.backpressure.should_pause_gemini_tasks():
                return True

            # 队列过长时拒绝低优先级任务
            if len(self.gemini_queue) > 100 and priority >= 7:
                return True

        # 根据背压等级拒绝低优先级任务
        if self.backpressure.should_reject_low_priority_tasks() and priority >= 6:
            return True

        return False

    def mark_task_completed(self, task: ScheduledTask, execution_time: float):
        """标记任务完成"""
        self.stats["tasks_completed"] += 1

        # 记录执行时间用于学习
        task_type_key = f"{task.task_type.value}_p{task.priority}"
        self.task_history[task_type_key].append(execution_time)

        # 保持历史记录大小
        if len(self.task_history[task_type_key]) > 100:
            self.task_history[task_type_key] = self.task_history[task_type_key][-50:]

        logger.debug(f"✅ Task {task.task_id} completed in {execution_time:.2f}s")

    def mark_task_failed(self, task: ScheduledTask, should_retry: bool = True):
        """标记任务失败"""
        if should_retry and task.retry_count < task.max_retries:
            # 重新调度任务
            task.retry_count += 1
            task.scheduled_time = time.time() + (2**task.retry_count)  # 指数退避

            if task.task_type == TaskType.GITHUB_FILE:
                heapq.heappush(self.github_queue, task)
                self.stats["github_queue_size"] = len(self.github_queue)
            else:
                heapq.heappush(self.gemini_queue, task)
                self.stats["gemini_queue_size"] = len(self.gemini_queue)

            logger.warning(f"🔄 Retrying task {task.task_id} (attempt {task.retry_count + 1}/{task.max_retries + 1})")
        else:
            self.stats["tasks_failed"] += 1
            logger.error(f"❌ Task {task.task_id} failed permanently")

    async def update_worker_limits(self):
        """更新worker数量限制"""
        await self.backpressure.evaluate_backpressure()

        # 获取建议的worker数量
        recommended_github = self.quota_monitor.get_recommended_github_workers()
        recommended_gemini = self.quota_monitor.get_recommended_gemini_workers()

        # 应用背压限制
        max_github_bp = self.backpressure.get_max_concurrent_github_workers()
        max_gemini_bp = self.backpressure.get_max_concurrent_gemini_workers()

        # 取较小值
        new_github_limit = min(recommended_github, max_github_bp)
        new_gemini_limit = min(recommended_gemini, max_gemini_bp)

        # 更新限制
        if new_github_limit != self.max_github_workers:
            logger.info(f"🔧 GitHub worker limit changed: {self.max_github_workers} -> {new_github_limit}")
            self.max_github_workers = new_github_limit

        if new_gemini_limit != self.max_gemini_workers:
            logger.info(f"🔧 Gemini worker limit changed: {self.max_gemini_workers} -> {new_gemini_limit}")
            self.max_gemini_workers = new_gemini_limit

    def can_start_github_worker(self) -> bool:
        """是否可以启动新的GitHub worker"""
        return self.active_github_workers < self.max_github_workers

    def can_start_gemini_worker(self) -> bool:
        """是否可以启动新的Gemini worker"""
        return self.active_gemini_workers < self.max_gemini_workers

    def register_github_worker_start(self):
        """注册GitHub worker启动"""
        self.active_github_workers += 1
        logger.debug(f"👷 GitHub worker started, active: {self.active_github_workers}/{self.max_github_workers}")

    def register_github_worker_stop(self):
        """注册GitHub worker停止"""
        self.active_github_workers = max(0, self.active_github_workers - 1)
        logger.debug(f"👷 GitHub worker stopped, active: {self.active_github_workers}/{self.max_github_workers}")

    def register_gemini_worker_start(self):
        """注册Gemini worker启动"""
        self.active_gemini_workers += 1
        logger.debug(f"🔐 Gemini worker started, active: {self.active_gemini_workers}/{self.max_gemini_workers}")

    def register_gemini_worker_stop(self):
        """注册Gemini worker停止"""
        self.active_gemini_workers = max(0, self.active_gemini_workers - 1)
        logger.debug(f"🔐 Gemini worker stopped, active: {self.active_gemini_workers}/{self.max_gemini_workers}")

    def _update_avg_wait_time(self, wait_time: float):
        """更新平均等待时间"""
        alpha = 0.1  # 指数移动平均权重
        self.stats["avg_wait_time"] = alpha * wait_time + (1 - alpha) * self.stats["avg_wait_time"]

    def get_predicted_execution_time(self, task_type: TaskType, priority: int) -> float:
        """预测任务执行时间"""
        task_type_key = f"{task_type.value}_p{priority}"
        history = self.task_history.get(task_type_key, [])

        if not history:
            # 没有历史数据，返回默认值
            if task_type == TaskType.GITHUB_FILE:
                return 3.0
            # GEMINI_VALIDATION
            return 5.0

        # 返回最近执行时间的平均值
        recent_times = history[-10:]  # 最近10次
        return sum(recent_times) / len(recent_times)

    def get_queue_status(self) -> dict:
        """获取队列状态"""
        current_time = time.time()

        # 计算等待中的任务数
        github_waiting = sum(1 for task in self.github_queue if task.scheduled_time <= current_time)
        gemini_waiting = sum(1 for task in self.gemini_queue if task.scheduled_time <= current_time)

        return {
            "github_queue_size": len(self.github_queue),
            "gemini_queue_size": len(self.gemini_queue),
            "github_waiting": github_waiting,
            "gemini_waiting": gemini_waiting,
            "active_github_workers": self.active_github_workers,
            "active_gemini_workers": self.active_gemini_workers,
            "max_github_workers": self.max_github_workers,
            "max_gemini_workers": self.max_gemini_workers,
            "stats": self.stats.copy(),
        }

    def clear_old_tasks(self, max_age_hours: int = 1):
        """清理过期任务"""
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600

        # 清理GitHub队列
        old_github_queue = self.github_queue
        self.github_queue = [task for task in old_github_queue if current_time - task.created_time < max_age_seconds]
        heapq.heapify(self.github_queue)

        # 清理Gemini队列
        old_gemini_queue = self.gemini_queue
        self.gemini_queue = [task for task in old_gemini_queue if current_time - task.created_time < max_age_seconds]
        heapq.heapify(self.gemini_queue)

        removed_count = (len(old_github_queue) - len(self.github_queue)) + (
            len(old_gemini_queue) - len(self.gemini_queue)
        )

        if removed_count > 0:
            logger.info(f"🧹 Cleared {removed_count} expired tasks")
            self.stats["github_queue_size"] = len(self.github_queue)
            self.stats["gemini_queue_size"] = len(self.gemini_queue)
