"""
Token感知的工作窃取算法
实现基于API token约束的工作窃取调度，优化worker利用率
"""

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum

from common.Logger import logger

from .adaptive_scheduler import ScheduledTask, TaskType
from .quota_monitor import QuotaMonitor
from .token_manager import TokenManager


@dataclass
class WorkerInfo:
    """Worker信息"""

    worker_id: str
    worker_type: TaskType
    assigned_token: str | None = None
    current_task: ScheduledTask | None = None
    task_queue: deque = field(default_factory=lambda: deque(maxlen=50))
    is_active: bool = True
    last_activity: float = field(default_factory=time.time)
    total_processed: int = 0
    total_stolen: int = 0  # 被窃取的任务数
    total_steals: int = 0  # 窃取的任务数

    @property
    def queue_size(self) -> int:
        return len(self.task_queue)

    @property
    def is_busy(self) -> bool:
        return self.current_task is not None

    @property
    def is_overloaded(self) -> bool:
        return self.queue_size > 20  # 队列超过20个任务认为过载

    @property
    def can_steal_from(self) -> bool:
        return self.queue_size > 5  # 队列超过5个任务时允许被窃取


class TokenConstraint(Enum):
    """Token约束类型"""

    SAME_TOKEN = "same_token"  # 必须使用相同token
    COMPATIBLE_TOKEN = "compatible"  # 可以使用兼容token
    ANY_TOKEN = "any_token"  # 可以使用任意token


@dataclass
class StealingRule:
    """窃取规则"""

    max_steal_ratio: float = 0.5  # 最多窃取目标队列的50%
    min_queue_size_to_steal: int = 3  # 目标队列至少3个任务才能窃取
    steal_batch_size: int = 2  # 每次窃取的任务数量
    steal_cooldown: float = 30.0  # 窃取冷却时间(秒)
    priority_threshold: int = 6  # 只窃取优先级>=6的任务(低优先级)


class WorkStealingScheduler:
    """Token感知的工作窃取调度器"""

    def __init__(self, quota_monitor: QuotaMonitor, token_manager: TokenManager):
        self.quota_monitor = quota_monitor
        self.token_manager = token_manager

        # Worker管理
        self.workers: dict[str, WorkerInfo] = {}
        self.github_workers: dict[str, WorkerInfo] = {}
        self.gemini_workers: dict[str, WorkerInfo] = {}

        # Token分配
        self.token_workers: dict[str, list[str]] = defaultdict(list)  # token -> worker_ids
        self.worker_tokens: dict[str, str] = {}  # worker_id -> token

        # 窃取规则和统计
        self.stealing_rules = StealingRule()
        self.last_steal_attempts: dict[str, float] = {}  # worker_id -> last_steal_time
        self.steal_success_rate: dict[str, float] = defaultdict(lambda: 0.5)  # worker_id -> success_rate

        # 锁和同步
        self.workers_lock = threading.RLock()
        self.steal_lock = threading.Lock()

        # 统计信息
        self.stats = {
            "total_steals_attempted": 0,
            "total_steals_successful": 0,
            "total_tasks_redistributed": 0,
            "avg_queue_balance": 0.0,
            "token_utilization": defaultdict(float),
        }

        logger.info("🎯 WorkStealingScheduler initialized")

    def register_worker(self, worker_id: str, worker_type: TaskType, preferred_token: str | None = None) -> bool:
        """
        注册新worker

        Args:
            worker_id: Worker ID
            worker_type: Worker类型
            preferred_token: 首选token

        Returns:
            bool: 注册是否成功
        """
        with self.workers_lock:
            if worker_id in self.workers:
                logger.warning(f"🔄 Worker {worker_id} already registered, updating...")

            # 分配token
            assigned_token = self._assign_token_to_worker(worker_id, worker_type, preferred_token)

            # 创建worker信息
            worker_info = WorkerInfo(worker_id=worker_id, worker_type=worker_type, assigned_token=assigned_token)

            # 注册到相应的集合
            self.workers[worker_id] = worker_info

            if worker_type == TaskType.GITHUB_FILE:
                self.github_workers[worker_id] = worker_info
            else:  # GEMINI_VALIDATION
                self.gemini_workers[worker_id] = worker_info

            # 更新token-worker映射
            if assigned_token:
                self.token_workers[assigned_token].append(worker_id)
                self.worker_tokens[worker_id] = assigned_token

            logger.info(
                f"👷 Registered {worker_type.value} worker {worker_id} with token {assigned_token[:10] if assigned_token else 'None'}..."
            )
            return True

    def unregister_worker(self, worker_id: str):
        """注销worker"""
        with self.workers_lock:
            if worker_id not in self.workers:
                return

            worker_info = self.workers[worker_id]

            # 分发剩余任务给其他worker
            if worker_info.task_queue:
                tasks_to_redistribute = list(worker_info.task_queue)
                worker_info.task_queue.clear()

                for task in tasks_to_redistribute:
                    self._redistribute_task(task, exclude_worker=worker_id)

                logger.info(f"📦 Redistributed {len(tasks_to_redistribute)} tasks from departing worker {worker_id}")

            # 从映射中移除
            assigned_token = worker_info.assigned_token
            if assigned_token and worker_id in self.token_workers[assigned_token]:
                self.token_workers[assigned_token].remove(worker_id)

            if worker_id in self.worker_tokens:
                del self.worker_tokens[worker_id]

            # 从worker集合中移除
            if worker_info.worker_type == TaskType.GITHUB_FILE:
                self.github_workers.pop(worker_id, None)
            else:
                self.gemini_workers.pop(worker_id, None)

            del self.workers[worker_id]

            logger.info(f"👋 Unregistered worker {worker_id}")

    def assign_task_to_worker(self, task: ScheduledTask) -> str | None:
        """
        将任务分配给最合适的worker

        Args:
            task: 调度任务

        Returns:
            Optional[str]: 分配到的worker ID，None表示无可用worker
        """
        with self.workers_lock:
            # 选择对应类型的worker集合
            if task.task_type == TaskType.GITHUB_FILE:
                candidate_workers = self.github_workers
            else:
                candidate_workers = self.gemini_workers

            if not candidate_workers:
                return None

            # 筛选可用worker
            available_workers = []
            for worker_id, worker_info in candidate_workers.items():
                if not worker_info.is_active:
                    continue

                # 检查token兼容性
                if task.task_type == TaskType.GITHUB_FILE:
                    if not self._is_token_compatible_for_task(worker_info.assigned_token, task):
                        continue

                # 计算worker负载得分
                load_score = self._calculate_worker_load_score(worker_info)
                available_workers.append((load_score, worker_id))

            if not available_workers:
                return None

            # 选择负载最低的worker
            available_workers.sort(key=lambda x: x[0])
            selected_worker_id = available_workers[0][1]

            # 将任务添加到worker队列
            worker_info = candidate_workers[selected_worker_id]
            worker_info.task_queue.append(task)
            worker_info.last_activity = time.time()

            logger.debug(f"📋 Assigned task {task.task_id} to worker {selected_worker_id}")
            return selected_worker_id

    def get_next_task_for_worker(self, worker_id: str) -> ScheduledTask | None:
        """
        为worker获取下一个任务（包括窃取逻辑）

        Args:
            worker_id: Worker ID

        Returns:
            Optional[ScheduledTask]: 下一个任务或None
        """
        with self.workers_lock:
            if worker_id not in self.workers:
                return None

            worker_info = self.workers[worker_id]

            # 1. 首先检查自己的队列
            if worker_info.task_queue:
                task = worker_info.task_queue.popleft()
                worker_info.current_task = task
                worker_info.last_activity = time.time()
                return task

            # 2. 自己队列为空，尝试窃取任务
            stolen_task = self._attempt_work_stealing(worker_id)
            if stolen_task:
                worker_info.current_task = stolen_task
                worker_info.last_activity = time.time()
                worker_info.total_steals += 1
                return stolen_task

            return None

    def mark_task_completed(self, worker_id: str, task: ScheduledTask):
        """标记任务完成"""
        with self.workers_lock:
            if worker_id in self.workers:
                worker_info = self.workers[worker_id]
                worker_info.current_task = None
                worker_info.total_processed += 1
                worker_info.last_activity = time.time()

    def _assign_token_to_worker(self, worker_id: str, worker_type: TaskType, preferred_token: str | None) -> str | None:
        """为worker分配token"""
        if worker_type == TaskType.GEMINI_VALIDATION:
            return None  # Gemini worker不需要GitHub token

        available_tokens = self.quota_monitor.github_tokens.copy()

        # 优先使用首选token
        if preferred_token and preferred_token in available_tokens:
            quota_status = self.quota_monitor.github_quota_status.get(preferred_token)
            if quota_status and quota_status.is_healthy:
                return preferred_token

        # 选择最佳token
        best_token = self.token_manager.get_best_github_token(task_priority=5)
        return best_token

    def _is_token_compatible_for_task(self, worker_token: str | None, task: ScheduledTask) -> bool:
        """检查worker的token是否兼容任务"""
        if not worker_token:
            return False

        # 检查token健康状态
        quota_status = self.quota_monitor.github_quota_status.get(worker_token)
        if not quota_status or not quota_status.is_healthy:
            return False

        # 高优先级任务需要更健康的token
        if task.priority <= 2:  # 高优先级
            return quota_status.remaining_ratio > 0.2
        if task.priority <= 5:  # 中等优先级
            return quota_status.remaining_ratio > 0.1
        # 低优先级
        return quota_status.remaining_ratio > 0.05

    def _calculate_worker_load_score(self, worker_info: WorkerInfo) -> float:
        """计算worker负载得分（越低越好）"""
        base_score = worker_info.queue_size

        # 当前是否有任务
        if worker_info.is_busy:
            base_score += 5

        # Token健康度调整
        if worker_info.assigned_token:
            quota_status = self.quota_monitor.github_quota_status.get(worker_info.assigned_token)
            if quota_status:
                health_penalty = (1 - quota_status.health_score) * 10
                base_score += health_penalty

        # 历史性能调整
        steal_success = self.steal_success_rate.get(worker_info.worker_id, 0.5)
        performance_bonus = (steal_success - 0.5) * 2  # -1 到 1
        base_score -= performance_bonus

        return base_score

    def _attempt_work_stealing(self, stealing_worker_id: str) -> ScheduledTask | None:
        """
        尝试工作窃取

        Args:
            stealing_worker_id: 窃取者worker ID

        Returns:
            Optional[ScheduledTask]: 窃取到的任务或None
        """
        current_time = time.time()

        # 检查冷却时间
        last_steal_time = self.last_steal_attempts.get(stealing_worker_id, 0)
        if current_time - last_steal_time < self.stealing_rules.steal_cooldown:
            return None

        stealing_worker = self.workers.get(stealing_worker_id)
        if not stealing_worker:
            return None

        # 获取同类型worker作为窃取目标
        if stealing_worker.worker_type == TaskType.GITHUB_FILE:
            target_workers = self.github_workers
        else:
            target_workers = self.gemini_workers

        # 寻找合适的窃取目标
        steal_candidates = []

        for target_worker_id, target_worker in target_workers.items():
            if target_worker_id == stealing_worker_id:
                continue

            if not target_worker.can_steal_from:
                continue

            # Token兼容性检查（仅对GitHub worker）
            if stealing_worker.worker_type == TaskType.GITHUB_FILE:
                if not self._can_steal_between_workers(stealing_worker, target_worker):
                    continue

            # 计算窃取价值
            steal_value = self._calculate_steal_value(stealing_worker, target_worker)
            if steal_value > 0:
                steal_candidates.append((steal_value, target_worker_id))

        if not steal_candidates:
            self.last_steal_attempts[stealing_worker_id] = current_time
            return None

        # 选择最佳窃取目标
        steal_candidates.sort(reverse=True)
        target_worker_id = steal_candidates[0][1]
        target_worker = target_workers[target_worker_id]

        # 执行窃取
        stolen_task = self._steal_task_from_worker(target_worker, stealing_worker)

        self.last_steal_attempts[stealing_worker_id] = current_time
        self.stats["total_steals_attempted"] += 1

        if stolen_task:
            self.stats["total_steals_successful"] += 1
            target_worker.total_stolen += 1
            logger.debug(f"🎯 Worker {stealing_worker_id} stole task {stolen_task.task_id} from {target_worker_id}")

            # 更新成功率
            current_success = self.steal_success_rate[stealing_worker_id]
            self.steal_success_rate[stealing_worker_id] = 0.9 * current_success + 0.1 * 1.0
        else:
            # 更新失败率
            current_success = self.steal_success_rate[stealing_worker_id]
            self.steal_success_rate[stealing_worker_id] = 0.9 * current_success + 0.1 * 0.0

        return stolen_task

    def _can_steal_between_workers(self, stealing_worker: WorkerInfo, target_worker: WorkerInfo) -> bool:
        """检查两个worker之间是否可以窃取任务"""
        stealing_token = stealing_worker.assigned_token
        target_token = target_worker.assigned_token

        if not stealing_token or not target_token:
            return False

        # 相同token可以窃取
        if stealing_token == target_token:
            return True

        # 检查窃取者token是否比目标token更健康
        stealing_status = self.quota_monitor.github_quota_status.get(stealing_token)
        target_status = self.quota_monitor.github_quota_status.get(target_token)

        if not stealing_status or not target_status:
            return False

        # 窃取者token配额更充足时可以窃取
        return stealing_status.remaining_ratio > target_status.remaining_ratio + 0.1

    def _calculate_steal_value(self, stealing_worker: WorkerInfo, target_worker: WorkerInfo) -> float:
        """计算窃取价值"""
        # 基础价值：目标worker过载程度
        base_value = max(0, target_worker.queue_size - 10) / 10

        # Token健康度差异加成
        if stealing_worker.worker_type == TaskType.GITHUB_FILE:
            stealing_token = stealing_worker.assigned_token
            target_token = target_worker.assigned_token

            if stealing_token and target_token:
                stealing_status = self.quota_monitor.github_quota_status.get(stealing_token)
                target_status = self.quota_monitor.github_quota_status.get(target_token)

                if stealing_status and target_status:
                    health_diff = stealing_status.health_score - target_status.health_score
                    base_value += health_diff * 0.5

        # 历史窃取成功率加成
        success_rate = self.steal_success_rate.get(stealing_worker.worker_id, 0.5)
        base_value *= 0.5 + success_rate

        return base_value

    def _steal_task_from_worker(self, target_worker: WorkerInfo, stealing_worker: WorkerInfo) -> ScheduledTask | None:
        """从目标worker窃取任务"""
        if not target_worker.task_queue:
            return None

        # 寻找合适的窃取任务（从队列末尾开始，优先窃取低优先级任务）
        queue_list = list(target_worker.task_queue)

        for i in range(len(queue_list) - 1, -1, -1):
            task = queue_list[i]

            # 检查是否可以窃取这个任务
            if task.priority >= self.stealing_rules.priority_threshold:
                # 检查token兼容性
                if stealing_worker.worker_type == TaskType.GITHUB_FILE:
                    if not self._is_token_compatible_for_task(stealing_worker.assigned_token, task):
                        continue

                # 移除任务并返回
                del target_worker.task_queue[i]
                return task

        return None

    def _redistribute_task(self, task: ScheduledTask, exclude_worker: str | None = None):
        """重新分配任务"""
        # 选择对应类型的worker
        if task.task_type == TaskType.GITHUB_FILE:
            candidate_workers = self.github_workers
        else:
            candidate_workers = self.gemini_workers

        available_workers = [
            (self._calculate_worker_load_score(worker), worker_id)
            for worker_id, worker in candidate_workers.items()
            if worker_id != exclude_worker and worker.is_active
        ]

        if available_workers:
            available_workers.sort()
            selected_worker_id = available_workers[0][1]
            candidate_workers[selected_worker_id].task_queue.append(task)
            self.stats["total_tasks_redistributed"] += 1

    def get_scheduler_stats(self) -> dict:
        """获取调度器统计信息"""
        with self.workers_lock:
            total_queue_size = sum(worker.queue_size for worker in self.workers.values())
            total_workers = len(self.workers)

            # 计算队列平衡度
            if total_workers > 0:
                avg_queue_size = total_queue_size / total_workers
                queue_variance = (
                    sum((worker.queue_size - avg_queue_size) ** 2 for worker in self.workers.values()) / total_workers
                )
                balance_score = 1.0 / (1.0 + queue_variance)  # 方差越小平衡度越高
            else:
                balance_score = 0.0

            # Token利用率
            token_stats = {}
            for token in self.quota_monitor.github_tokens:
                workers_with_token = len(self.token_workers.get(token, []))
                quota_status = self.quota_monitor.github_quota_status.get(token)
                utilization = workers_with_token / max(1, len(self.github_workers)) if self.github_workers else 0

                token_stats[token[:10] + "..."] = {
                    "workers": workers_with_token,
                    "utilization": utilization,
                    "health_score": quota_status.health_score if quota_status else 0.0,
                }

            return {
                "total_workers": total_workers,
                "github_workers": len(self.github_workers),
                "gemini_workers": len(self.gemini_workers),
                "total_queue_size": total_queue_size,
                "avg_queue_size": total_queue_size / max(1, total_workers),
                "queue_balance_score": balance_score,
                "steal_stats": self.stats.copy(),
                "token_distribution": token_stats,
                "worker_details": {
                    worker_id: {
                        "type": worker.worker_type.value,
                        "queue_size": worker.queue_size,
                        "is_busy": worker.is_busy,
                        "total_processed": worker.total_processed,
                        "total_stolen": worker.total_stolen,
                        "total_steals": worker.total_steals,
                        "assigned_token": worker.assigned_token[:10] + "..." if worker.assigned_token else None,
                    }
                    for worker_id, worker in self.workers.items()
                },
            }
