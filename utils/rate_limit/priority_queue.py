"""
API配额感知的优先级队列系统
根据API配额状态和任务特性动态计算任务优先级
"""

import heapq
import re
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from common.Logger import logger


class TaskPriority(Enum):
    """任务优先级等级"""

    CRITICAL = 0  # 紧急任务
    HIGH = 1  # 高优先级
    MEDIUM = 5  # 中等优先级
    LOW = 7  # 低优先级
    BACKGROUND = 9  # 后台任务


@dataclass
class FileTaskInfo:
    """文件任务信息"""

    repo_name: str
    file_path: str
    file_size: int
    repo_stars: int
    repo_updated_at: str
    file_url: str
    content_preview: str | None = None

    @property
    def estimated_key_density(self) -> float:
        """估算API key密度"""
        if not self.content_preview:
            # 基于文件扩展名估算
            if self.file_path.endswith((".js", ".ts", ".py", ".env", ".config")):
                return 0.7
            if self.file_path.endswith((".json", ".yaml", ".yml")):
                return 0.8
            if self.file_path.endswith((".md", ".txt", ".rst")):
                return 0.2
            return 0.5

        # 基于内容分析
        api_key_pattern = r"AIzaSy[A-Za-z0-9\-_]{33}"
        potential_keys = len(re.findall(api_key_pattern, self.content_preview))
        content_length = len(self.content_preview)

        if content_length == 0:
            return 0.0

        # 密度 = 潜在key数量 / 内容长度 * 1000 (标准化)
        density = (potential_keys / content_length) * 1000
        return min(1.0, density)

    @property
    def repo_freshness_score(self) -> float:
        """仓库新鲜度评分"""
        try:
            updated_dt = datetime.strptime(self.repo_updated_at, "%Y-%m-%dT%H:%M:%SZ")
            days_since_update = (datetime.utcnow() - updated_dt).days

            # 越新鲜分数越高
            if days_since_update <= 7:
                return 1.0
            if days_since_update <= 30:
                return 0.8
            if days_since_update <= 90:
                return 0.6
            if days_since_update <= 365:
                return 0.4
            return 0.2
        except (ValueError, TypeError, KeyError):
            return 0.5  # 默认分数

    @property
    def repo_popularity_score(self) -> float:
        """仓库流行度评分"""
        if self.repo_stars >= 10000:
            return 1.0
        if self.repo_stars >= 1000:
            return 0.8
        if self.repo_stars >= 100:
            return 0.6
        if self.repo_stars >= 10:
            return 0.4
        return 0.2


@dataclass
class KeyValidationTaskInfo:
    """Key验证任务信息"""

    key: str
    repo_name: str
    file_path: str
    repo_stars: int
    key_source_context: str  # key周围的上下文

    @property
    def key_confidence_score(self) -> float:
        """Key可信度评分"""
        # 基于上下文分析key的可信度
        context_lower = self.key_source_context.lower()

        # 负面指标
        if any(
            keyword in context_lower for keyword in ["example", "sample", "demo", "test", "placeholder", "your_api_key"]
        ):
            return 0.1

        if "..." in self.key_source_context:
            return 0.2

        # 正面指标
        confidence = 0.5  # 基础分数

        if any(keyword in context_lower for keyword in ["api_key", "google_api", "gemini", "generative"]):
            confidence += 0.3

        if re.search(r'["\']' + re.escape(self.key) + r'["\']', self.key_source_context):
            confidence += 0.2  # 被引号包围

        return min(1.0, confidence)


class PriorityCalculator:
    """优先级计算器"""

    def __init__(self):
        # 权重配置
        self.weights = {
            "api_quota_efficiency": 0.35,  # API配额效率
            "expected_success_rate": 0.25,  # 预期成功率
            "repo_value": 0.20,  # 仓库价值
            "task_urgency": 0.15,  # 任务紧急度
            "resource_cost": 0.05,  # 资源成本
        }

    def calculate_file_task_priority(
        self, task_info: FileTaskInfo, current_quota_status: dict, task_age_seconds: float = 0
    ) -> int:
        """
        计算文件任务优先级

        Args:
            task_info: 文件任务信息
            current_quota_status: 当前配额状态
            task_age_seconds: 任务已等待时间

        Returns:
            int: 优先级分数 (0-9, 0最高)
        """

        # 1. API配额效率 - 每个API调用预期获得的key数量
        key_density = task_info.estimated_key_density
        file_size_kb = task_info.file_size / 1024

        # 文件越小、key密度越高，效率越高
        if file_size_kb <= 10:  # 小文件
            size_efficiency = 1.0
        elif file_size_kb <= 100:  # 中等文件
            size_efficiency = 0.7
        else:  # 大文件
            size_efficiency = 0.4

        api_quota_efficiency = key_density * size_efficiency

        # 2. 预期成功率
        # 基于仓库质量和文件类型
        repo_quality = task_info.repo_popularity_score * 0.6 + task_info.repo_freshness_score * 0.4

        # 文件类型成功率
        if task_info.file_path.endswith(".env"):
            file_type_success = 0.9
        elif task_info.file_path.endswith((".config", ".json", ".yaml")):
            file_type_success = 0.8
        elif task_info.file_path.endswith((".js", ".ts", ".py")):
            file_type_success = 0.7
        else:
            file_type_success = 0.5

        expected_success_rate = repo_quality * 0.7 + file_type_success * 0.3

        # 3. 仓库价值
        repo_value = (task_info.repo_popularity_score + task_info.repo_freshness_score) / 2

        # 4. 任务紧急度 - 基于等待时间和配额状态
        urgency_base = min(task_age_seconds / 3600, 1.0)  # 等待时间影响

        # 配额充足时降低紧急度，配额紧张时提高紧急度
        github_quota_ratio = current_quota_status.get("github", {}).get("avg_health_score", 0.5)
        if github_quota_ratio > 0.7:
            urgency_multiplier = 0.8  # 配额充足，不着急
        elif github_quota_ratio < 0.3:
            urgency_multiplier = 1.5  # 配额紧张，抓紧处理高价值任务
        else:
            urgency_multiplier = 1.0

        task_urgency = urgency_base * urgency_multiplier

        # 5. 资源成本 - 文件大小和预期处理时间
        processing_time_estimate = max(1, file_size_kb / 100)  # 估算处理时间
        resource_cost = 1.0 / processing_time_estimate  # 成本越低分数越高

        # 综合计算
        weighted_score = (
            api_quota_efficiency * self.weights["api_quota_efficiency"]
            + expected_success_rate * self.weights["expected_success_rate"]
            + repo_value * self.weights["repo_value"]
            + task_urgency * self.weights["task_urgency"]
            + resource_cost * self.weights["resource_cost"]
        )

        # 转换为优先级等级 (0-9)
        if weighted_score >= 0.8:
            return TaskPriority.CRITICAL.value
        if weighted_score >= 0.65:
            return TaskPriority.HIGH.value
        if weighted_score >= 0.4:
            return TaskPriority.MEDIUM.value
        if weighted_score >= 0.2:
            return TaskPriority.LOW.value
        return TaskPriority.BACKGROUND.value

    def calculate_validation_task_priority(
        self, task_info: KeyValidationTaskInfo, current_quota_status: dict, task_age_seconds: float = 0
    ) -> int:
        """
        计算key验证任务优先级

        Args:
            task_info: Key验证任务信息
            current_quota_status: 当前配额状态
            task_age_seconds: 任务已等待时间

        Returns:
            int: 优先级分数 (0-9, 0最高)
        """

        # 1. Key可信度 - 这个key有效的可能性
        key_confidence = task_info.key_confidence_score

        # 2. 仓库价值
        if task_info.repo_stars >= 1000:
            repo_value = 1.0
        elif task_info.repo_stars >= 100:
            repo_value = 0.8
        elif task_info.repo_stars >= 10:
            repo_value = 0.6
        else:
            repo_value = 0.4

        # 3. 验证紧急度 - 基于Gemini配额状态
        gemini_health = current_quota_status.get("gemini", {}).get("success_rate", 0.8)
        recent_429 = current_quota_status.get("gemini", {}).get("recent_429", False)

        if recent_429:
            urgency_multiplier = 0.5  # 最近有429，降低优先级
        elif gemini_health > 0.9:
            urgency_multiplier = 1.2  # 验证状态很好，提高优先级
        else:
            urgency_multiplier = 1.0

        urgency = min(task_age_seconds / 1800, 1.0) * urgency_multiplier  # 30分钟内线性增长

        # 4. API效率 - 高可信度key优先验证，避免浪费配额
        api_efficiency = key_confidence

        # 综合计算
        weighted_score = key_confidence * 0.4 + repo_value * 0.3 + urgency * 0.2 + api_efficiency * 0.1

        # 转换为优先级等级
        if weighted_score >= 0.8:
            return TaskPriority.CRITICAL.value
        if weighted_score >= 0.6:
            return TaskPriority.HIGH.value
        if weighted_score >= 0.4:
            return TaskPriority.MEDIUM.value
        if weighted_score >= 0.2:
            return TaskPriority.LOW.value
        return TaskPriority.BACKGROUND.value


class QuotaAwarePriorityQueue:
    """API配额感知的优先级队列"""

    def __init__(self, queue_type: str, max_size: int = 1000):
        self.queue_type = queue_type  # 'github' or 'gemini'
        self.max_size = max_size
        self.queue: list[tuple[int, float, str, Any]] = []  # (priority, timestamp, task_id, task_data)
        self.priority_calculator = PriorityCalculator()

        # 统计信息
        self.stats = {
            "total_added": 0,
            "total_removed": 0,
            "priority_distribution": dict.fromkeys(range(10), 0),
            "queue_full_rejections": 0,
            "reprioritizations": 0,
        }

        logger.info(f"📊 QuotaAwarePriorityQueue initialized for {queue_type}, max_size: {max_size}")

    def add_file_task(
        self, task_id: str, task_info: FileTaskInfo, current_quota_status: dict, task_data: Any = None
    ) -> bool:
        """
        添加文件任务到队列

        Args:
            task_id: 任务ID
            task_info: 文件任务信息
            current_quota_status: 当前配额状态
            task_data: 任务数据

        Returns:
            bool: 是否成功添加
        """
        if len(self.queue) >= self.max_size:
            self.stats["queue_full_rejections"] += 1
            logger.warning(f"📦 {self.queue_type} queue full, rejecting task {task_id}")
            return False

        # 计算优先级
        priority = self.priority_calculator.calculate_file_task_priority(task_info, current_quota_status)

        # 添加到队列
        timestamp = time.time()
        heapq.heappush(self.queue, (priority, timestamp, task_id, task_data))

        # 更新统计
        self.stats["total_added"] += 1
        self.stats["priority_distribution"][priority] += 1

        logger.debug(f"📥 Added {self.queue_type} task {task_id} with priority {priority}")
        return True

    def add_validation_task(
        self, task_id: str, task_info: KeyValidationTaskInfo, current_quota_status: dict, task_data: Any = None
    ) -> bool:
        """
        添加验证任务到队列

        Args:
            task_id: 任务ID
            task_info: Key验证任务信息
            current_quota_status: 当前配额状态
            task_data: 任务数据

        Returns:
            bool: 是否成功添加
        """
        if len(self.queue) >= self.max_size:
            self.stats["queue_full_rejections"] += 1
            logger.warning(f"📦 {self.queue_type} queue full, rejecting validation task {task_id}")
            return False

        # 计算优先级
        priority = self.priority_calculator.calculate_validation_task_priority(task_info, current_quota_status)

        # 添加到队列
        timestamp = time.time()
        heapq.heappush(self.queue, (priority, timestamp, task_id, task_data))

        # 更新统计
        self.stats["total_added"] += 1
        self.stats["priority_distribution"][priority] += 1

        logger.debug(f"📥 Added validation task {task_id} with priority {priority}")
        return True

    def get_next_task(self) -> tuple[str, Any] | None:
        """
        获取下一个任务

        Returns:
            Optional[Tuple[str, Any]]: (task_id, task_data) 或 None
        """
        if not self.queue:
            return None

        priority, timestamp, task_id, task_data = heapq.heappop(self.queue)

        # 更新统计
        self.stats["total_removed"] += 1
        self.stats["priority_distribution"][priority] -= 1

        logger.debug(f"📤 Retrieved {self.queue_type} task {task_id} with priority {priority}")
        return task_id, task_data

    def peek_next_task(self) -> tuple[int, str] | None:
        """
        查看下一个任务但不移除

        Returns:
            Optional[Tuple[int, str]]: (priority, task_id) 或 None
        """
        if not self.queue:
            return None

        priority, timestamp, task_id, task_data = self.queue[0]
        return priority, task_id

    def reprioritize_tasks(self, current_quota_status: dict):
        """
        重新计算所有任务的优先级

        Args:
            current_quota_status: 当前配额状态
        """
        if not self.queue:
            return

        # 提取所有任务
        old_tasks = []
        while self.queue:
            priority, timestamp, task_id, task_data = heapq.heappop(self.queue)
            old_tasks.append((timestamp, task_id, task_data))

        # 重新计算优先级并添加回队列
        current_time = time.time()
        for timestamp, task_id, task_data in old_tasks:
            task_age = current_time - timestamp

            # 这里需要根据task_data的类型来重新计算优先级
            # 简化实现：保持原有优先级但添加年龄加成
            age_bonus = min(int(task_age / 600), 2)  # 每10分钟降低1个优先级等级，最多2级
            new_priority = max(0, priority - age_bonus)

            heapq.heappush(self.queue, (new_priority, timestamp, task_id, task_data))

        self.stats["reprioritizations"] += 1
        logger.info(f"🔄 Reprioritized {len(old_tasks)} tasks in {self.queue_type} queue")

    def clear_low_priority_tasks(self, min_priority: int = 7) -> int:
        """
        清理低优先级任务

        Args:
            min_priority: 最低保留优先级

        Returns:
            int: 清理的任务数量
        """
        if not self.queue:
            return 0

        # 提取所有任务
        all_tasks = []
        while self.queue:
            all_tasks.append(heapq.heappop(self.queue))

        # 过滤高优先级任务
        kept_tasks = []
        removed_count = 0

        for priority, timestamp, task_id, task_data in all_tasks:
            if priority < min_priority:  # 保留高优先级任务 (数字越小优先级越高)
                kept_tasks.append((priority, timestamp, task_id, task_data))
            else:
                removed_count += 1
                # 更新统计
                self.stats["priority_distribution"][priority] -= 1

        # 重新构建队列
        for task in kept_tasks:
            heapq.heappush(self.queue, task)

        if removed_count > 0:
            logger.info(f"🧹 Cleared {removed_count} low priority tasks from {self.queue_type} queue")

        return removed_count

    def get_queue_stats(self) -> dict:
        """获取队列统计信息"""
        priority_counts = {
            f"priority_{i}": count for i, count in self.stats["priority_distribution"].items() if count > 0
        }

        return {
            "queue_type": self.queue_type,
            "current_size": len(self.queue),
            "max_size": self.max_size,
            "total_added": self.stats["total_added"],
            "total_removed": self.stats["total_removed"],
            "queue_full_rejections": self.stats["queue_full_rejections"],
            "reprioritizations": self.stats["reprioritizations"],
            "priority_distribution": priority_counts,
        }
