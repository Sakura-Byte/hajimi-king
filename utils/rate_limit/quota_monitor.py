"""
API配额监控模块
实时监控GitHub API和Gemini API的配额使用情况
"""

import asyncio
import time
from typing import Dict, List, Optional, NamedTuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import aiohttp
import requests

from common.Logger import logger
from common.config import Config


@dataclass
class QuotaStatus:
    """API配额状态"""
    remaining: int
    limit: int
    reset_time: int
    usage_rate: float = 0.0  # 使用率 (0-1)
    error_rate: float = 0.0  # 错误率 (0-1)
    last_429_time: Optional[float] = None  # 最后一次429错误时间
    
    @property
    def remaining_ratio(self) -> float:
        """剩余配额比例"""
        return self.remaining / self.limit if self.limit > 0 else 0.0
    
    @property
    def is_healthy(self) -> bool:
        """Token是否健康"""
        return (
            self.remaining_ratio > 0.1 and  # 剩余>10%
            self.error_rate < 0.2 and       # 错误率<20%
            (not self.last_429_time or time.time() - self.last_429_time > 300)  # 5分钟内无429
        )
    
    @property
    def health_score(self) -> float:
        """健康度评分 (0-1)"""
        base_score = self.remaining_ratio * 0.6 + (1 - self.error_rate) * 0.4
        
        # 429惩罚
        if self.last_429_time:
            time_since_429 = time.time() - self.last_429_time
            penalty = max(0, 1 - time_since_429 / 300)  # 5分钟内线性恢复
            base_score *= (1 - penalty * 0.5)
        
        return max(0, min(1, base_score))


@dataclass 
class APIMetrics:
    """API调用指标"""
    total_calls: int = 0
    successful_calls: int = 0
    error_calls: int = 0
    rate_limit_hits: int = 0
    avg_response_time: float = 0.0
    last_call_time: Optional[float] = None
    
    @property
    def success_rate(self) -> float:
        if self.total_calls == 0:
            return 1.0
        return self.successful_calls / self.total_calls
    
    @property 
    def error_rate(self) -> float:
        return 1.0 - self.success_rate


class QuotaMonitor:
    """API配额监控器"""
    
    def __init__(self):
        self.github_tokens = Config.GITHUB_TOKENS.copy()
        self.github_quota_status: Dict[str, QuotaStatus] = {}
        self.github_metrics: Dict[str, APIMetrics] = {}
        
        # Gemini API监控 (简化版，因为没有直接的配额查询API)
        self.gemini_metrics = APIMetrics()
        self.gemini_last_429 = None
        
        # 监控配置
        self.github_warning_threshold = float(Config.parse_bool(getattr(Config, 'GITHUB_QUOTA_WARNING_THRESHOLD', '30'))) / 100
        self.github_critical_threshold = float(Config.parse_bool(getattr(Config, 'GITHUB_QUOTA_CRITICAL_THRESHOLD', '10'))) / 100
        
        # 初始化token状态
        for token in self.github_tokens:
            self.github_quota_status[token] = QuotaStatus(remaining=5000, limit=5000, reset_time=int(time.time()) + 3600)
            self.github_metrics[token] = APIMetrics()
        
        logger.info(f"📊 QuotaMonitor initialized for {len(self.github_tokens)} GitHub tokens")
    
    async def update_github_quota(self, token: str, headers: Dict[str, str]):
        """从API响应头更新GitHub配额状态"""
        try:
            remaining = int(headers.get('X-RateLimit-Remaining', 0))
            limit = int(headers.get('X-RateLimit-Limit', 5000))
            reset_time = int(headers.get('X-RateLimit-Reset', time.time() + 3600))
            
            old_status = self.github_quota_status.get(token)
            
            self.github_quota_status[token] = QuotaStatus(
                remaining=remaining,
                limit=limit,
                reset_time=reset_time,
                error_rate=old_status.error_rate if old_status else 0.0,
                last_429_time=old_status.last_429_time if old_status else None
            )
            
            # 更新使用率
            if old_status and old_status.remaining > remaining:
                calls_made = old_status.remaining - remaining
                time_elapsed = max(1, time.time() - (old_status.last_call_time or time.time()))
                self.github_quota_status[token].usage_rate = calls_made / time_elapsed
            
            # 检查是否需要警告
            quota_ratio = remaining / limit
            if quota_ratio < self.github_critical_threshold:
                logger.warning(f"🔴 GitHub token {token[:10]}... critical quota: {remaining}/{limit} ({quota_ratio*100:.1f}%)")
            elif quota_ratio < self.github_warning_threshold:
                logger.warning(f"🟡 GitHub token {token[:10]}... low quota: {remaining}/{limit} ({quota_ratio*100:.1f}%)")
                
        except (ValueError, TypeError) as e:
            logger.error(f"❌ Failed to parse GitHub quota headers: {e}")
    
    def record_github_call(self, token: str, success: bool, response_time: float, is_429: bool = False):
        """记录GitHub API调用"""
        if token not in self.github_metrics:
            self.github_metrics[token] = APIMetrics()
        
        metrics = self.github_metrics[token]
        metrics.total_calls += 1
        metrics.last_call_time = time.time()
        
        if success:
            metrics.successful_calls += 1
        else:
            metrics.error_calls += 1
        
        if is_429:
            metrics.rate_limit_hits += 1
            if token in self.github_quota_status:
                self.github_quota_status[token].last_429_time = time.time()
        
        # 更新平均响应时间 (指数移动平均)
        alpha = 0.1
        metrics.avg_response_time = alpha * response_time + (1 - alpha) * metrics.avg_response_time
        
        # 更新错误率 (基于最近100次调用)
        if token in self.github_quota_status:
            self.github_quota_status[token].error_rate = metrics.error_rate
    
    def record_gemini_call(self, success: bool, response_time: float, is_429: bool = False):
        """记录Gemini API调用"""
        self.gemini_metrics.total_calls += 1
        self.gemini_metrics.last_call_time = time.time()
        
        if success:
            self.gemini_metrics.successful_calls += 1
        else:
            self.gemini_metrics.error_calls += 1
        
        if is_429:
            self.gemini_metrics.rate_limit_hits += 1
            self.gemini_last_429 = time.time()
        
        # 更新平均响应时间
        alpha = 0.1
        self.gemini_metrics.avg_response_time = alpha * response_time + (1 - alpha) * self.gemini_metrics.avg_response_time
    
    def get_best_github_token(self) -> Optional[str]:
        """获取最佳GitHub token"""
        if not self.github_quota_status:
            return self.github_tokens[0] if self.github_tokens else None
        
        # 计算每个token的综合得分
        token_scores = []
        for token, status in self.github_quota_status.items():
            if status.remaining <= 0:
                continue  # 跳过已耗尽的token
            
            score = status.health_score
            token_scores.append((score, token))
        
        if not token_scores:
            # 所有token都耗尽，返回重置时间最近的
            return min(
                self.github_quota_status.keys(),
                key=lambda t: self.github_quota_status[t].reset_time
            )
        
        # 返回得分最高的token
        return max(token_scores)[1]
    
    def get_quota_summary(self) -> Dict:
        """获取配额使用摘要"""
        github_summary = {
            'total_tokens': len(self.github_tokens),
            'healthy_tokens': sum(1 for status in self.github_quota_status.values() if status.is_healthy),
            'total_remaining': sum(status.remaining for status in self.github_quota_status.values()),
            'avg_health_score': sum(status.health_score for status in self.github_quota_status.values()) / len(self.github_quota_status) if self.github_quota_status else 0,
            'tokens_in_danger': sum(1 for status in self.github_quota_status.values() if status.remaining_ratio < self.github_warning_threshold)
        }
        
        gemini_summary = {
            'total_calls': self.gemini_metrics.total_calls,
            'success_rate': self.gemini_metrics.success_rate,
            'recent_429': self.gemini_last_429 is not None and time.time() - self.gemini_last_429 < 300,
            'avg_response_time': self.gemini_metrics.avg_response_time
        }
        
        return {
            'github': github_summary,
            'gemini': gemini_summary,
            'overall_health': min(github_summary['avg_health_score'], 0.8 if not gemini_summary['recent_429'] else 0.3)
        }
    
    def should_reduce_github_workers(self) -> bool:
        """是否应该减少GitHub worker数量"""
        summary = self.get_quota_summary()
        return (
            summary['github']['avg_health_score'] < 0.3 or
            summary['github']['healthy_tokens'] < len(self.github_tokens) * 0.5 or
            summary['github']['tokens_in_danger'] > len(self.github_tokens) * 0.7
        )
    
    def should_reduce_gemini_workers(self) -> bool:
        """是否应该减少Gemini worker数量"""
        return (
            self.gemini_metrics.error_rate > 0.3 or
            (self.gemini_last_429 and time.time() - self.gemini_last_429 < 60)  # 1分钟内有429
        )
    
    def get_recommended_github_workers(self) -> int:
        """获取推荐的GitHub worker数量"""
        summary = self.get_quota_summary()
        
        base_workers = summary['github']['healthy_tokens']
        
        # 根据健康度调整
        health_multiplier = min(2.0, summary['github']['avg_health_score'] * 2)
        
        # 根据剩余配额调整
        avg_remaining_ratio = summary['github']['total_remaining'] / (len(self.github_tokens) * 5000) if self.github_tokens else 0
        quota_multiplier = min(1.5, avg_remaining_ratio * 2)
        
        recommended = int(base_workers * health_multiplier * quota_multiplier)
        
        # 限制范围
        min_workers = getattr(Config, 'MIN_FILE_WORKERS_PER_TOKEN', 1) * len(self.github_tokens)
        max_workers = getattr(Config, 'MAX_FILE_WORKERS_PER_TOKEN', 3) * len(self.github_tokens)
        
        return max(min_workers, min(max_workers, recommended))
    
    def get_recommended_gemini_workers(self) -> int:
        """获取推荐的Gemini worker数量"""
        base_workers = 5  # 默认基础worker数
        
        if self.gemini_metrics.error_rate > 0.2:
            return max(2, base_workers // 2)  # 错误率高时减半
        
        if self.gemini_last_429 and time.time() - self.gemini_last_429 < 300:
            return max(2, base_workers // 3)  # 5分钟内有429时减少2/3
        
        return base_workers