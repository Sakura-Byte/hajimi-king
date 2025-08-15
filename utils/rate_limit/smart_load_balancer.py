"""
基于API配额的智能负载均衡器
综合管理GitHub tokens、proxies和Gemini API的智能分配和负载均衡
"""

import random
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import statistics

from common.Logger import logger
from common.config import Config
from .quota_monitor import QuotaMonitor
from .token_manager import TokenManager


class ResourceType(Enum):
    """资源类型"""
    GITHUB_TOKEN = "github_token"
    PROXY = "proxy"
    GEMINI_KEY = "gemini_key"


@dataclass
class ResourceHealth:
    """资源健康状态"""
    resource_id: str
    resource_type: ResourceType
    health_score: float = 1.0    # 健康度评分 0-1
    availability: float = 1.0    # 可用性 0-1
    response_time: float = 0.0   # 平均响应时间
    error_rate: float = 0.0      # 错误率 0-1
    load_factor: float = 0.0     # 负载因子 0-1
    last_used: float = 0.0       # 最后使用时间
    consecutive_errors: int = 0   # 连续错误次数
    recovery_time: float = 0.0   # 恢复时间估算
    
    @property
    def is_healthy(self) -> bool:
        return self.health_score > 0.3 and self.availability > 0.5
    
    @property
    def is_overloaded(self) -> bool:
        return self.load_factor > 0.8
    
    @property
    def needs_rest(self) -> bool:
        return self.consecutive_errors > 3 or self.error_rate > 0.3


@dataclass 
class LoadBalancingDecision:
    """负载均衡决策"""
    resource_id: str
    resource_type: ResourceType
    confidence: float      # 决策置信度 0-1
    expected_latency: float # 预期延迟
    alternative_resources: List[str] = field(default_factory=list)
    decision_reason: str = ""


class PredictiveLoadBalancer:
    """预测性负载均衡器"""
    
    def __init__(self, quota_monitor: QuotaMonitor, token_manager: TokenManager):
        self.quota_monitor = quota_monitor
        self.token_manager = token_manager
        
        # 资源健康状态追踪
        self.resource_health: Dict[str, ResourceHealth] = {}
        
        # 历史性能数据 (用于预测)
        self.performance_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.latency_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        
        # 负载均衡策略 (未使用，已移除)
        
        # 预测模型参数
        self.prediction_window = 300  # 5分钟预测窗口
        self.load_smoothing_factor = 0.1  # 负载平滑因子
        
        # 故障转移配置
        self.failover_threshold = 0.2  # 健康度低于此值触发故障转移
        self.recovery_period = 300     # 资源恢复观察期(秒)
        
        # 统计信息
        self.stats = {
            'total_decisions': 0,
            'failover_events': 0,
            'load_redistributions': 0,
            'prediction_accuracy': 0.0,
            'avg_response_time': defaultdict(float),
            'resource_utilization': defaultdict(float)
        }
        
        # 初始化资源健康状态
        self._initialize_resource_health()
        
        logger.info("🎯 PredictiveLoadBalancer initialized")
    
    def _initialize_resource_health(self):
        """初始化所有资源的健康状态"""
        # GitHub tokens
        for token in self.quota_monitor.github_tokens:
            self.resource_health[token] = ResourceHealth(
                resource_id=token,
                resource_type=ResourceType.GITHUB_TOKEN
            )
        
        # Proxies
        for i, proxy in enumerate(Config.PROXY_LIST):
            proxy_id = f"proxy_{i}_{hash(proxy) % 1000}"
            self.resource_health[proxy_id] = ResourceHealth(
                resource_id=proxy_id,
                resource_type=ResourceType.PROXY
            )
        
        # Gemini (简化为单一资源)
        self.resource_health["gemini_primary"] = ResourceHealth(
            resource_id="gemini_primary",
            resource_type=ResourceType.GEMINI_KEY
        )
    
    async def get_best_github_token(self, task_priority: int = 5, predicted_load: float = 1.0) -> Optional[LoadBalancingDecision]:
        """
        获取最佳GitHub token
        
        Args:
            task_priority: 任务优先级
            predicted_load: 预期负载
        
        Returns:
            LoadBalancingDecision: 负载均衡决策
        """
        self.stats['total_decisions'] += 1
        
        # 更新所有GitHub token的健康状态
        await self._update_github_token_health()
        
        # 获取候选token
        candidates = []
        for token in self.quota_monitor.github_tokens:
            health = self.resource_health.get(token)
            if not health or not health.is_healthy:
                continue
            
            # 计算token得分
            score = self._calculate_github_token_score(token, task_priority, predicted_load)
            candidates.append((score, token))
        
        if not candidates:
            # 没有健康的token，尝试故障转移
            return await self._handle_github_token_failover(task_priority)
        
        # 选择最佳token
        candidates.sort(reverse=True)
        best_score, best_token = candidates[0]
        
        # 预测延迟
        predicted_latency = self._predict_token_latency(best_token, predicted_load)
        
        # 构建决策
        decision = LoadBalancingDecision(
            resource_id=best_token,
            resource_type=ResourceType.GITHUB_TOKEN,
            confidence=min(1.0, best_score),
            expected_latency=predicted_latency,
            alternative_resources=[token for _, token in candidates[1:3]],  # 前2个备选
            decision_reason=f"Best score: {best_score:.3f}, predicted latency: {predicted_latency:.2f}s"
        )
        
        # 更新使用记录
        self.resource_health[best_token].last_used = time.time()
        self.resource_health[best_token].load_factor = min(1.0, self.resource_health[best_token].load_factor + predicted_load * 0.1)
        
        return decision
    
    async def get_best_proxy(self, task_context: str = "") -> Optional[LoadBalancingDecision]:
        """获取最佳代理"""
        if not Config.PROXY_LIST:
            return None
        
        await self._update_proxy_health()
        
        # 计算代理得分
        candidates = []
        for proxy_id, health in self.resource_health.items():
            if health.resource_type != ResourceType.PROXY or not health.is_healthy:
                continue
            
            score = health.health_score * 0.6 + (1 - health.load_factor) * 0.4
            candidates.append((score, proxy_id))
        
        if not candidates:
            # 随机选择一个代理作为fallback
            fallback_proxy = f"proxy_fallback_{random.randint(0, len(Config.PROXY_LIST)-1)}"
            return LoadBalancingDecision(
                resource_id=fallback_proxy,
                resource_type=ResourceType.PROXY,
                confidence=0.3,
                expected_latency=10.0,
                decision_reason="Fallback proxy due to no healthy proxies"
            )
        
        candidates.sort(reverse=True)
        best_score, best_proxy_id = candidates[0]
        
        decision = LoadBalancingDecision(
            resource_id=best_proxy_id,
            resource_type=ResourceType.PROXY,
            confidence=min(1.0, best_score),
            expected_latency=self.resource_health[best_proxy_id].response_time,
            alternative_resources=[proxy_id for _, proxy_id in candidates[1:2]],
            decision_reason=f"Best proxy score: {best_score:.3f}"
        )
        
        return decision
    
    def record_resource_performance(self, resource_id: str, success: bool, response_time: float, error_type: str = ""):
        """
        记录资源性能数据
        
        Args:
            resource_id: 资源ID
            success: 是否成功
            response_time: 响应时间
            error_type: 错误类型
        """
        if resource_id not in self.resource_health:
            return
        
        health = self.resource_health[resource_id]
        current_time = time.time()
        
        # 更新响应时间
        self.latency_history[resource_id].append(response_time)
        if self.latency_history[resource_id]:
            health.response_time = statistics.mean(self.latency_history[resource_id])
        
        # 更新成功/失败记录
        self.performance_history[resource_id].append(1 if success else 0)
        
        if success:
            health.consecutive_errors = 0
            # 逐渐恢复健康度
            health.health_score = min(1.0, health.health_score + 0.02)
        else:
            health.consecutive_errors += 1
            # 降低健康度
            penalty = 0.1 if error_type in ["timeout", "connection_error"] else 0.05
            health.health_score = max(0.0, health.health_score - penalty)
            
            # 429错误特殊处理
            if "429" in error_type:
                health.health_score = max(0.0, health.health_score - 0.2)
                health.recovery_time = current_time + 300  # 5分钟恢复期
        
        # 更新错误率
        recent_performance = list(self.performance_history[resource_id])[-20:]  # 最近20次
        if recent_performance:
            health.error_rate = 1.0 - (sum(recent_performance) / len(recent_performance))
        
        # 更新可用性
        if health.consecutive_errors > 5:
            health.availability = 0.0
        elif health.consecutive_errors > 2:
            health.availability = 0.5
        else:
            health.availability = 1.0
        
        # 更新统计
        resource_type = health.resource_type.value
        self.stats['avg_response_time'][resource_type] = (
            self.stats['avg_response_time'][resource_type] * 0.9 + response_time * 0.1
        )
    
    async def _update_github_token_health(self):
        """更新GitHub token健康状态"""
        for token in self.quota_monitor.github_tokens:
            if token not in self.resource_health:
                continue
            
            health = self.resource_health[token]
            quota_status = self.quota_monitor.github_quota_status.get(token)
            
            if quota_status:
                # 基于配额状态更新健康度
                quota_health = quota_status.health_score
                
                # 综合考虑配额健康度和历史性能
                health.health_score = quota_health * 0.7 + health.health_score * 0.3
                
                # 基于剩余配额计算负载因子
                health.load_factor = 1.0 - quota_status.remaining_ratio
                
                # 如果配额耗尽，设置恢复时间
                if quota_status.remaining <= 0:
                    health.recovery_time = quota_status.reset_time
                    health.availability = 0.0
    
    async def _update_proxy_health(self):
        """更新代理健康状态"""
        current_time = time.time()
        
        for proxy_id, health in self.resource_health.items():
            if health.resource_type != ResourceType.PROXY:
                continue
            
            # 如果长时间未使用，逐渐降低负载因子
            if current_time - health.last_used > 300:  # 5分钟
                health.load_factor = max(0.0, health.load_factor - 0.1)
            
            # 如果在恢复期内，检查是否可以恢复
            if health.recovery_time > 0 and current_time >= health.recovery_time:
                health.recovery_time = 0
                health.consecutive_errors = 0
                health.health_score = 0.5  # 重置为中等健康度
                logger.info(f"🔄 Proxy {proxy_id} recovered from failure")
    
    def _calculate_github_token_score(self, token: str, task_priority: int, predicted_load: float) -> float:
        """计算GitHub token得分"""
        health = self.resource_health.get(token)
        quota_status = self.quota_monitor.github_quota_status.get(token)
        
        if not health or not quota_status:
            return 0.0
        
        # 基础得分：健康度和配额状态
        base_score = health.health_score * 0.5 + quota_status.health_score * 0.5
        
        # 负载平衡调整
        load_penalty = health.load_factor * 0.3
        base_score -= load_penalty
        
        # 优先级调整
        if task_priority <= 2:  # 高优先级任务
            # 高优先级任务更偏好高配额token
            quota_bonus = quota_status.remaining_ratio * 0.2
            base_score += quota_bonus
        elif task_priority >= 7:  # 低优先级任务
            # 低优先级任务避免使用高配额token
            if quota_status.remaining_ratio > 0.7:
                base_score -= 0.1
        
        # 响应时间调整
        if health.response_time > 0:
            latency_penalty = min(0.2, health.response_time / 10)
            base_score -= latency_penalty
        
        # 预期负载调整
        future_load = health.load_factor + predicted_load * 0.1
        if future_load > 0.8:
            base_score -= 0.3  # 避免过载
        
        return max(0.0, min(1.0, base_score))
    
    def _predict_token_latency(self, token: str, predicted_load: float) -> float:
        """预测token响应延迟"""
        health = self.resource_health.get(token)
        if not health:
            return 5.0  # 默认延迟
        
        base_latency = health.response_time if health.response_time > 0 else 2.0
        
        # 根据负载调整延迟
        load_multiplier = 1.0 + health.load_factor * 0.5
        
        # 根据健康度调整
        health_multiplier = 2.0 - health.health_score  # 健康度越低延迟越高
        
        predicted_latency = base_latency * load_multiplier * health_multiplier
        
        return min(30.0, predicted_latency)  # 最大30秒延迟
    
    async def _handle_github_token_failover(self, task_priority: int) -> Optional[LoadBalancingDecision]:
        """处理GitHub token故障转移"""
        self.stats['failover_events'] += 1
        
        # 寻找恢复中的token
        recovering_tokens = []
        current_time = time.time()
        
        for token in self.quota_monitor.github_tokens:
            health = self.resource_health.get(token)
            if not health:
                continue
            
            # 检查是否在恢复期
            if health.recovery_time > 0 and current_time >= health.recovery_time:
                recovering_tokens.append(token)
        
        if recovering_tokens:
            # 选择一个恢复中的token
            best_token = min(recovering_tokens, key=lambda t: self.resource_health[t].consecutive_errors)
            
            return LoadBalancingDecision(
                resource_id=best_token,
                resource_type=ResourceType.GITHUB_TOKEN,
                confidence=0.3,
                expected_latency=10.0,
                decision_reason="Failover to recovering token"
            )
        
        # 选择最不坏的token
        least_bad_token = min(
            self.quota_monitor.github_tokens,
            key=lambda t: self.resource_health.get(t, ResourceHealth("", ResourceType.GITHUB_TOKEN)).consecutive_errors
        )
        
        return LoadBalancingDecision(
            resource_id=least_bad_token,
            resource_type=ResourceType.GITHUB_TOKEN,
            confidence=0.1,
            expected_latency=15.0,
            decision_reason="Emergency fallback to least-bad token"
        )
    
    def rebalance_load(self) -> Dict[str, Any]:
        """执行负载重新均衡"""
        self.stats['load_redistributions'] += 1
        
        rebalance_actions = []
        
        # GitHub token负载均衡
        github_actions = self._rebalance_github_tokens()
        rebalance_actions.extend(github_actions)
        
        # Proxy负载均衡
        proxy_actions = self._rebalance_proxies()
        rebalance_actions.extend(proxy_actions)
        
        logger.info(f"🔄 Load rebalancing completed with {len(rebalance_actions)} actions")
        
        return {
            'actions_taken': len(rebalance_actions),
            'actions_detail': rebalance_actions,
            'rebalance_timestamp': time.time()
        }
    
    def _rebalance_github_tokens(self) -> List[Dict]:
        """重新均衡GitHub token负载"""
        actions = []
        
        # 找出过载和空闲的token
        overloaded_tokens = []
        underutilized_tokens = []
        
        for token in self.quota_monitor.github_tokens:
            health = self.resource_health.get(token)
            if not health:
                continue
            
            if health.is_overloaded and health.is_healthy:
                overloaded_tokens.append(token)
            elif health.load_factor < 0.3 and health.is_healthy:
                underutilized_tokens.append(token)
        
        # 执行负载转移
        for overloaded_token in overloaded_tokens:
            if not underutilized_tokens:
                break
            
            target_token = underutilized_tokens.pop(0)
            
            # 减少过载token的负载
            self.resource_health[overloaded_token].load_factor *= 0.8
            # 增加目标token的负载
            self.resource_health[target_token].load_factor += 0.2
            
            actions.append({
                'type': 'load_transfer',
                'from': overloaded_token[:10] + "...",
                'to': target_token[:10] + "...",
                'amount': 0.2
            })
        
        return actions
    
    def _rebalance_proxies(self) -> List[Dict]:
        """重新均衡代理负载"""
        actions = []
        
        proxy_healths = [
            (proxy_id, health) for proxy_id, health in self.resource_health.items()
            if health.resource_type == ResourceType.PROXY
        ]
        
        if len(proxy_healths) < 2:
            return actions
        
        # 按健康度排序
        proxy_healths.sort(key=lambda x: x[1].health_score, reverse=True)
        
        # 如果最好和最坏的代理健康度差异很大，建议切换
        best_proxy = proxy_healths[0]
        worst_proxy = proxy_healths[-1]
        
        health_diff = best_proxy[1].health_score - worst_proxy[1].health_score
        
        if health_diff > 0.4:
            actions.append({
                'type': 'proxy_recommendation',
                'recommend': best_proxy[0],
                'avoid': worst_proxy[0],
                'reason': f'Health difference: {health_diff:.2f}'
            })
        
        return actions
    
    def get_load_balancer_stats(self) -> Dict:
        """获取负载均衡器统计信息"""
        resource_summary = defaultdict(lambda: {'count': 0, 'healthy': 0, 'avg_load': 0.0, 'avg_health': 0.0})
        
        for resource_id, health in self.resource_health.items():
            resource_type = health.resource_type.value
            resource_summary[resource_type]['count'] += 1
            
            if health.is_healthy:
                resource_summary[resource_type]['healthy'] += 1
            
            resource_summary[resource_type]['avg_load'] += health.load_factor
            resource_summary[resource_type]['avg_health'] += health.health_score
        
        # 计算平均值
        for resource_type in resource_summary:
            count = resource_summary[resource_type]['count']
            if count > 0:
                resource_summary[resource_type]['avg_load'] /= count
                resource_summary[resource_type]['avg_health'] /= count
        
        return {
            'resource_summary': dict(resource_summary),
            'global_stats': self.stats.copy(),
            'health_distribution': {
                'healthy': sum(1 for h in self.resource_health.values() if h.is_healthy),
                'unhealthy': sum(1 for h in self.resource_health.values() if not h.is_healthy),
                'recovering': sum(1 for h in self.resource_health.values() if h.recovery_time > time.time()),
                'overloaded': sum(1 for h in self.resource_health.values() if h.is_overloaded)
            },
            'top_performers': self._get_top_performing_resources(),
            'bottlenecks': self._identify_bottlenecks()
        }
    
    def _get_top_performing_resources(self) -> Dict[str, List[str]]:
        """获取性能最佳的资源"""
        by_type = defaultdict(list)
        
        for resource_id, health in self.resource_health.items():
            by_type[health.resource_type.value].append((health.health_score, resource_id))
        
        top_performers = {}
        for resource_type, resources in by_type.items():
            resources.sort(reverse=True)
            top_performers[resource_type] = [resource_id for _, resource_id in resources[:3]]
        
        return top_performers
    
    def _identify_bottlenecks(self) -> List[Dict]:
        """识别性能瓶颈"""
        bottlenecks = []
        
        for resource_id, health in self.resource_health.items():
            issues = []
            
            if health.health_score < 0.3:
                issues.append("low_health")
            if health.error_rate > 0.2:
                issues.append("high_error_rate")
            if health.response_time > 10:
                issues.append("high_latency")
            if health.is_overloaded:
                issues.append("overloaded")
            
            if issues:
                bottlenecks.append({
                    'resource_id': resource_id[:20] + "..." if len(resource_id) > 20 else resource_id,
                    'resource_type': health.resource_type.value,
                    'issues': issues,
                    'severity': len(issues) / 4.0
                })
        
        return sorted(bottlenecks, key=lambda x: x['severity'], reverse=True)[:5]