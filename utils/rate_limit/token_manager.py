"""
Token管理模块
智能管理GitHub tokens和其他API keys的使用
"""

import random
import time
from collections import deque
from dataclasses import dataclass

from common.config import Config
from common.Logger import logger

from .quota_monitor import QuotaMonitor


@dataclass
class TokenUsageHistory:
    """Token使用历史记录"""

    recent_calls: deque  # 最近调用时间队列
    recent_errors: deque  # 最近错误时间队列
    total_usage: int = 0
    last_reset_time: float = 0
    consecutive_errors: int = 0

    def __post_init__(self):
        if not hasattr(self, "recent_calls"):
            self.recent_calls = deque(maxlen=100)
        if not hasattr(self, "recent_errors"):
            self.recent_errors = deque(maxlen=50)


class TokenManager:
    """智能Token管理器"""

    def __init__(self, quota_monitor: QuotaMonitor):
        self.quota_monitor = quota_monitor
        self.github_tokens = Config.GITHUB_TOKENS.copy()

        # Token使用历史
        self.token_history: dict[str, TokenUsageHistory] = {}
        for token in self.github_tokens:
            self.token_history[token] = TokenUsageHistory(
                recent_calls=deque(maxlen=100), recent_errors=deque(maxlen=50)
            )

        # Token轮换策略
        self.current_token_index = 0
        self.token_weights: dict[str, float] = dict.fromkeys(self.github_tokens, 1.0)

        # 代理管理
        self.proxy_list = Config.PROXY_LIST.copy() if Config.PROXY_LIST else []
        self.proxy_performance: dict[str, float] = dict.fromkeys(self.proxy_list, 1.0)

        # 休眠中的token
        self.sleeping_tokens: dict[str, float] = {}  # token -> wake_up_time

        logger.info(
            f"🎯 TokenManager initialized with {len(self.github_tokens)} GitHub tokens and {len(self.proxy_list)} proxies"
        )

    def get_best_github_token(self, task_priority: int = 5) -> str | None:
        """
        获取最佳GitHub token

        Args:
            task_priority: 任务优先级 (0-9, 0最高)
        """
        available_tokens = []

        # 移除正在休眠且未到唤醒时间的token
        current_time = time.time()
        for token in list(self.sleeping_tokens.keys()):
            if current_time >= self.sleeping_tokens[token]:
                del self.sleeping_tokens[token]
                logger.info(f"😴 Token {token[:10]}... woke up from sleep")

        # 筛选可用token
        for token in self.github_tokens:
            if token in self.sleeping_tokens:
                continue

            quota_status = self.quota_monitor.github_quota_status.get(token)
            if not quota_status:
                available_tokens.append((0.5, token))  # 未知状态给中等分数
                continue

            # 计算token可用性分数
            score = self._calculate_token_score(token, quota_status, task_priority)

            if score > 0.1:  # 最低可用阈值
                available_tokens.append((score, token))

        if not available_tokens:
            # 没有可用token，选择最快恢复的
            if self.sleeping_tokens:
                best_token = min(self.sleeping_tokens.keys(), key=lambda t: self.sleeping_tokens[t])
                logger.warning(
                    f"⏰ All tokens unavailable, will use {best_token[:10]}... (wakes up in {self.sleeping_tokens[best_token] - current_time:.0f}s)"
                )
                return best_token
            # 从配额状态选择最好的
            return self.quota_monitor.get_best_github_token()

        # 根据分数加权随机选择
        if len(available_tokens) == 1:
            return available_tokens[0][1]

        # 加权随机选择，避免总是选择最好的token
        total_weight = sum(score for score, _ in available_tokens)
        rand_val = random.uniform(0, total_weight)

        cumulative = 0
        for score, token in sorted(available_tokens, reverse=True):
            cumulative += score
            if rand_val <= cumulative:
                return token

        return available_tokens[0][1]  # fallback

    def _calculate_token_score(self, token: str, quota_status, task_priority: int) -> float:
        """
        计算token评分

        Args:
            token: GitHub token
            quota_status: 配额状态
            task_priority: 任务优先级
        """
        history = self.token_history[token]

        # 基础分数：剩余配额比例
        base_score = quota_status.remaining_ratio

        # 健康度加成
        health_bonus = quota_status.health_score * 0.3

        # 错误率惩罚
        error_penalty = quota_status.error_rate * 0.4

        # 连续错误惩罚
        consecutive_error_penalty = min(history.consecutive_errors * 0.1, 0.5)

        # 使用频率平衡 (避免某个token被过度使用)
        current_time = time.time()
        recent_usage = sum(1 for call_time in history.recent_calls if current_time - call_time < 300)  # 5分钟内使用次数
        usage_penalty = min(recent_usage * 0.02, 0.3)

        # 优先级调整：高优先级任务可以使用配额较少的token
        if task_priority <= 3 and quota_status.remaining_ratio > 0.05:  # 高优先级且有基本配额
            priority_bonus = 0.2
        elif task_priority >= 7 and quota_status.remaining_ratio < 0.3:  # 低优先级避免使用紧张的token
            priority_bonus = -0.3
        else:
            priority_bonus = 0

        final_score = (
            base_score + health_bonus - error_penalty - consecutive_error_penalty - usage_penalty + priority_bonus
        )

        return max(0, min(1, final_score))

    def record_token_usage(self, token: str, success: bool, response_time: float, is_429: bool = False):
        """记录token使用情况"""
        if token not in self.token_history:
            return

        history = self.token_history[token]
        current_time = time.time()

        # 记录调用
        history.recent_calls.append(current_time)
        history.total_usage += 1

        if not success:
            history.recent_errors.append(current_time)
            history.consecutive_errors += 1

            # 429错误特殊处理
            if is_429:
                self._handle_rate_limit(token)
        else:
            history.consecutive_errors = 0

        # 更新token权重
        self._update_token_weight(token, success, response_time, is_429)

        # 同时更新quota monitor
        self.quota_monitor.record_github_call(token, success, response_time, is_429)

    def _handle_rate_limit(self, token: str):
        """处理rate limit情况"""
        quota_status = self.quota_monitor.github_quota_status.get(token)
        if not quota_status:
            return

        # 计算休眠时间
        reset_time = quota_status.reset_time
        current_time = time.time()

        if reset_time > current_time:
            # token进入休眠状态直到重置
            sleep_duration = min(reset_time - current_time, 3600)  # 最多休眠1小时
            self.sleeping_tokens[token] = current_time + sleep_duration
            logger.warning(f"😴 Token {token[:10]}... going to sleep for {sleep_duration:.0f}s due to rate limit")

        # 降低token权重
        self.token_weights[token] *= 0.5

    def _update_token_weight(self, token: str, success: bool, response_time: float, is_429: bool):
        """更新token权重"""
        current_weight = self.token_weights[token]

        if success:
            # 成功调用增加权重
            self.token_weights[token] = min(2.0, current_weight * 1.05)
        else:
            # 失败调用降低权重
            penalty = 0.8 if is_429 else 0.9
            self.token_weights[token] = max(0.1, current_weight * penalty)

        # 响应时间调整
        if response_time > 10:  # 响应太慢
            self.token_weights[token] *= 0.95
        elif response_time < 2:  # 响应很快
            self.token_weights[token] = min(2.0, self.token_weights[token] * 1.02)

    def get_best_proxy(self) -> dict[str, str] | None:
        """获取最佳代理"""
        if not self.proxy_list:
            return None

        # 根据性能评分选择代理
        best_proxy = max(self.proxy_list, key=lambda p: self.proxy_performance.get(p, 1.0))

        # 添加一些随机性，避免总是使用同一个代理
        if random.random() < 0.3:  # 30%概率随机选择
            best_proxy = random.choice(self.proxy_list)

        return {"http": best_proxy, "https": best_proxy}

    def record_proxy_performance(self, proxy: str, success: bool, response_time: float):
        """记录代理性能"""
        if proxy not in self.proxy_performance:
            self.proxy_performance[proxy] = 1.0

        current_score = self.proxy_performance[proxy]

        if success:
            # 成功且响应快
            if response_time < 5:
                self.proxy_performance[proxy] = min(2.0, current_score * 1.1)
            else:
                self.proxy_performance[proxy] = min(2.0, current_score * 1.05)
        else:
            # 失败
            self.proxy_performance[proxy] = max(0.1, current_score * 0.8)

    def force_sleep_token(self, token: str, duration: int = 300):
        """强制token休眠指定时间"""
        self.sleeping_tokens[token] = time.time() + duration
        logger.info(f"😴 Forced token {token[:10]}... to sleep for {duration}s")

    def wake_up_token(self, token: str):
        """立即唤醒token"""
        if token in self.sleeping_tokens:
            del self.sleeping_tokens[token]
            logger.info(f"😴 Token {token[:10]}... manually woken up")

    def get_token_status_summary(self) -> dict:
        """获取token状态摘要"""
        current_time = time.time()

        active_tokens = [t for t in self.github_tokens if t not in self.sleeping_tokens]
        sleeping_tokens = len(self.sleeping_tokens)

        avg_weight = sum(self.token_weights.values()) / len(self.token_weights) if self.token_weights else 0

        recent_usage = {}
        for token, history in self.token_history.items():
            recent_count = sum(1 for call_time in history.recent_calls if current_time - call_time < 300)
            recent_usage[token] = recent_count

        return {
            "total_tokens": len(self.github_tokens),
            "active_tokens": len(active_tokens),
            "sleeping_tokens": sleeping_tokens,
            "avg_weight": avg_weight,
            "recent_usage": recent_usage,
            "best_token": self.get_best_github_token(),
            "proxy_count": len(self.proxy_list),
        }
