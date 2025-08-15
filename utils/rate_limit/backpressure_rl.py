"""
基于Rate Limit的背压控制模块
当API配额紧张时自动调整系统行为以避免过载
"""

import time
from dataclasses import dataclass
from enum import Enum

from common.config import Config
from common.Logger import logger

from .quota_monitor import QuotaMonitor
from .token_manager import TokenManager


class BackpressureLevel(Enum):
    """背压等级"""

    NONE = 0  # 无背压
    LOW = 1  # 轻微背压
    MEDIUM = 2  # 中等背压
    HIGH = 3  # 高背压
    CRITICAL = 4  # 临界背压


@dataclass
class BackpressureState:
    """背压状态"""

    level: BackpressureLevel
    github_pressure: float  # GitHub API压力 (0-1)
    gemini_pressure: float  # Gemini API压力 (0-1)
    active_since: float  # 开始时间
    reason: str  # 触发原因

    @property
    def duration(self) -> float:
        """背压持续时间(秒)"""
        return time.time() - self.active_since


class RateLimitBackpressure:
    """Rate Limit背压控制器"""

    def __init__(self, quota_monitor: QuotaMonitor, token_manager: TokenManager):
        self.quota_monitor = quota_monitor
        self.token_manager = token_manager

        # 当前背压状态
        self.current_state = BackpressureState(
            level=BackpressureLevel.NONE,
            github_pressure=0.0,
            gemini_pressure=0.0,
            active_since=time.time(),
            reason="initial",
        )

        # 配置阈值
        self.github_warning_threshold = 0.3  # GitHub配额30%以下警告
        self.github_critical_threshold = 0.1  # GitHub配额10%以下临界
        self.error_rate_threshold = 0.2  # 错误率20%以上
        self.rate_limit_hit_threshold = 0.1  # 10%的调用触发429

        # 背压策略配置
        self.backoff_multiplier = float(getattr(Config, "RATE_LIMIT_BACKOFF_MULTIPLIER", "2.0"))
        self.max_backoff_delay = 60.0  # 最大延迟60秒

        # 状态历史
        self.pressure_history: list[BackpressureState] = []
        self.max_history = 100

        logger.info("🛡️ RateLimitBackpressure initialized")

    async def evaluate_backpressure(self) -> BackpressureState:
        """评估当前背压情况"""
        github_pressure = self._calculate_github_pressure()
        gemini_pressure = self._calculate_gemini_pressure()

        # 确定背压等级
        overall_pressure = max(github_pressure, gemini_pressure)

        if overall_pressure >= 0.9:
            level = BackpressureLevel.CRITICAL
            reason = "Critical API quota exhaustion"
        elif overall_pressure >= 0.7:
            level = BackpressureLevel.HIGH
            reason = "High API pressure detected"
        elif overall_pressure >= 0.5:
            level = BackpressureLevel.MEDIUM
            reason = "Medium API pressure detected"
        elif overall_pressure >= 0.3:
            level = BackpressureLevel.LOW
            reason = "Low API pressure detected"
        else:
            level = BackpressureLevel.NONE
            reason = "Normal operation"

        # 更新状态
        if level != self.current_state.level:
            # 记录状态变化
            if len(self.pressure_history) >= self.max_history:
                self.pressure_history.pop(0)
            self.pressure_history.append(self.current_state)

            # 创建新状态
            self.current_state = BackpressureState(
                level=level,
                github_pressure=github_pressure,
                gemini_pressure=gemini_pressure,
                active_since=time.time(),
                reason=reason,
            )

            logger.info(f"🛡️ Backpressure level changed to {level.name}: {reason}")
        else:
            # 更新压力值但保持等级
            self.current_state.github_pressure = github_pressure
            self.current_state.gemini_pressure = gemini_pressure

        return self.current_state

    def _calculate_github_pressure(self) -> float:
        """计算GitHub API压力"""
        summary = self.quota_monitor.get_quota_summary()
        github_summary = summary["github"]

        pressure_factors = []

        # 1. 整体配额剩余比例
        if github_summary["total_tokens"] > 0:
            avg_remaining = github_summary["total_remaining"] / (github_summary["total_tokens"] * 5000)
            if avg_remaining < self.github_critical_threshold:
                pressure_factors.append(0.9)
            elif avg_remaining < self.github_warning_threshold:
                pressure_factors.append(0.6)
            else:
                pressure_factors.append(
                    max(0, (self.github_warning_threshold - avg_remaining) / self.github_warning_threshold)
                )

        # 2. 健康token比例
        healthy_ratio = github_summary["healthy_tokens"] / max(1, github_summary["total_tokens"])
        if healthy_ratio < 0.3:
            pressure_factors.append(0.8)
        elif healthy_ratio < 0.5:
            pressure_factors.append(0.5)
        else:
            pressure_factors.append(0)

        # 3. 危险token比例
        danger_ratio = github_summary["tokens_in_danger"] / max(1, github_summary["total_tokens"])
        pressure_factors.append(danger_ratio * 0.7)

        # 4. 最近429错误情况
        recent_429_count = 0
        current_time = time.time()
        for token in self.quota_monitor.github_tokens:
            quota_status = self.quota_monitor.github_quota_status.get(token)
            if quota_status and quota_status.last_429_time:
                if current_time - quota_status.last_429_time < 300:  # 5分钟内
                    recent_429_count += 1

        if recent_429_count > 0:
            rate_limit_pressure = min(0.9, recent_429_count / len(self.quota_monitor.github_tokens))
            pressure_factors.append(rate_limit_pressure)

        return min(1.0, max(pressure_factors) if pressure_factors else 0.0)

    def _calculate_gemini_pressure(self) -> float:
        """计算Gemini API压力"""
        metrics = self.quota_monitor.gemini_metrics
        pressure_factors = []

        # 1. 错误率
        if metrics.error_rate > self.error_rate_threshold:
            pressure_factors.append(min(0.8, metrics.error_rate * 2))

        # 2. 最近429情况
        if self.quota_monitor.gemini_last_429:
            time_since_429 = time.time() - self.quota_monitor.gemini_last_429
            if time_since_429 < 60:  # 1分钟内
                pressure_factors.append(0.9)
            elif time_since_429 < 300:  # 5分钟内
                pressure_factors.append(0.6)

        # 3. Rate limit命中率
        if metrics.total_calls > 10:  # 至少有一些调用数据
            rate_limit_rate = metrics.rate_limit_hits / metrics.total_calls
            if rate_limit_rate > self.rate_limit_hit_threshold:
                pressure_factors.append(min(0.7, rate_limit_rate * 5))

        return min(1.0, max(pressure_factors) if pressure_factors else 0.0)

    async def get_delay_for_github_task(self, priority: int = 5) -> float:
        """获取GitHub任务的延迟时间"""
        await self.evaluate_backpressure()

        base_delay = 0.0
        level = self.current_state.level

        if level == BackpressureLevel.NONE:
            return 0.0
        if level == BackpressureLevel.LOW:
            base_delay = 1.0
        elif level == BackpressureLevel.MEDIUM:
            base_delay = 3.0
        elif level == BackpressureLevel.HIGH:
            base_delay = 8.0
        elif level == BackpressureLevel.CRITICAL:
            base_delay = 20.0

        # 优先级调整
        if priority <= 2:  # 高优先级
            base_delay *= 0.5
        elif priority >= 7:  # 低优先级
            base_delay *= 2.0

        # 添加一些随机性避免所有worker同时请求
        jitter = base_delay * 0.2 * (2 * hash(str(time.time())) % 1000 / 1000 - 1)

        return min(self.max_backoff_delay, base_delay + jitter)

    async def get_delay_for_gemini_task(self, priority: int = 5) -> float:
        """获取Gemini任务的延迟时间"""
        await self.evaluate_backpressure()

        base_delay = 0.0
        level = self.current_state.level

        # Gemini API通常对rate limit更敏感
        if level == BackpressureLevel.NONE:
            return 0.5  # 最小延迟
        if level == BackpressureLevel.LOW:
            base_delay = 2.0
        elif level == BackpressureLevel.MEDIUM:
            base_delay = 5.0
        elif level == BackpressureLevel.HIGH:
            base_delay = 12.0
        elif level == BackpressureLevel.CRITICAL:
            base_delay = 30.0

        # 优先级调整
        if priority <= 2:
            base_delay *= 0.7
        elif priority >= 7:
            base_delay *= 1.5

        # Gemini特有的随机延迟
        import random

        jitter = random.uniform(0.5, 2.0)

        return min(self.max_backoff_delay, base_delay + jitter)

    def should_pause_github_tasks(self) -> bool:
        """是否应该暂停GitHub任务"""
        return self.current_state.level == BackpressureLevel.CRITICAL or self.current_state.github_pressure > 0.85

    def should_pause_gemini_tasks(self) -> bool:
        """是否应该暂停Gemini任务"""
        return self.current_state.level == BackpressureLevel.CRITICAL or self.current_state.gemini_pressure > 0.8

    def should_reject_low_priority_tasks(self) -> bool:
        """是否应该拒绝低优先级任务"""
        return self.current_state.level >= BackpressureLevel.MEDIUM

    def get_max_concurrent_github_workers(self) -> int:
        """获取GitHub最大并发worker数"""
        base_workers = len(self.quota_monitor.github_tokens) * 2  # 基础worker数

        if self.current_state.level == BackpressureLevel.NONE:
            return base_workers
        if self.current_state.level == BackpressureLevel.LOW:
            return max(2, int(base_workers * 0.8))
        if self.current_state.level == BackpressureLevel.MEDIUM:
            return max(2, int(base_workers * 0.6))
        if self.current_state.level == BackpressureLevel.HIGH:
            return max(1, int(base_workers * 0.4))
        # CRITICAL
        return 1

    def get_max_concurrent_gemini_workers(self) -> int:
        """获取Gemini最大并发worker数"""
        base_workers = 5

        if self.current_state.level == BackpressureLevel.NONE:
            return base_workers
        if self.current_state.level == BackpressureLevel.LOW:
            return max(2, int(base_workers * 0.7))
        if self.current_state.level == BackpressureLevel.MEDIUM:
            return max(2, int(base_workers * 0.5))
        if self.current_state.level == BackpressureLevel.HIGH:
            return max(1, int(base_workers * 0.3))
        # CRITICAL
        return 1

    def get_status_summary(self) -> dict:
        """获取背压状态摘要"""
        return {
            "level": self.current_state.level.name,
            "github_pressure": self.current_state.github_pressure,
            "gemini_pressure": self.current_state.gemini_pressure,
            "duration": self.current_state.duration,
            "reason": self.current_state.reason,
            "should_pause_github": self.should_pause_github_tasks(),
            "should_pause_gemini": self.should_pause_gemini_tasks(),
            "max_github_workers": self.get_max_concurrent_github_workers(),
            "max_gemini_workers": self.get_max_concurrent_gemini_workers(),
            "recent_state_changes": len(self.pressure_history),
        }
