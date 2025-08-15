import os
import random

from dotenv import load_dotenv

from common.Logger import logger

# 只在环境变量不存在时才从.env加载值
load_dotenv(override=False)


# Helper function for parsing boolean values
def parse_bool(value: str) -> bool:
    """
    解析布尔值配置，支持多种格式

    Args:
        value: 配置值字符串

    Returns:
        bool: 解析后的布尔值
    """
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        value = value.strip().lower()
        return value in ("true", "1", "yes", "on", "enabled")

    if isinstance(value, int):
        return bool(value)

    return False


class Config:
    GITHUB_TOKENS_STR = os.getenv("GITHUB_TOKENS", "")

    # 获取GitHub tokens列表
    GITHUB_TOKENS = [token.strip() for token in GITHUB_TOKENS_STR.split(",") if token.strip()]
    DATA_PATH = os.getenv("DATA_PATH", "/app/data")
    PROXY_LIST_STR = os.getenv("PROXY", "")

    # 解析代理列表，支持格式：http://user:pass@host:port,http://host:port,socks5://user:pass@host:port
    PROXY_LIST = []
    if PROXY_LIST_STR:
        for proxy_str in PROXY_LIST_STR.split(","):
            proxy_str = proxy_str.strip()
            if proxy_str:
                PROXY_LIST.append(proxy_str)

    # Gemini Balancer配置
    GEMINI_BALANCER_SYNC_ENABLED = os.getenv("GEMINI_BALANCER_SYNC_ENABLED", "false")
    GEMINI_BALANCER_URL = os.getenv("GEMINI_BALANCER_URL", "")
    GEMINI_BALANCER_AUTH = os.getenv("GEMINI_BALANCER_AUTH", "")

    # GPT Load Balancer Configuration
    GPT_LOAD_SYNC_ENABLED = os.getenv("GPT_LOAD_SYNC_ENABLED", "false")
    GPT_LOAD_URL = os.getenv("GPT_LOAD_URL", "")
    GPT_LOAD_AUTH = os.getenv("GPT_LOAD_AUTH", "")
    GPT_LOAD_GROUP_NAME = os.getenv("GPT_LOAD_GROUP_NAME", "")

    # 文件前缀配置
    VALID_KEY_PREFIX = os.getenv("VALID_KEY_PREFIX", "keys/keys_valid_")
    RATE_LIMITED_KEY_PREFIX = os.getenv("RATE_LIMITED_KEY_PREFIX", "keys/key_429_")
    KEYS_SEND_PREFIX = os.getenv("KEYS_SEND_PREFIX", "keys/keys_send_")

    VALID_KEY_DETAIL_PREFIX = os.getenv("VALID_KEY_DETAIL_PREFIX", "logs/keys_valid_detail_")
    RATE_LIMITED_KEY_DETAIL_PREFIX = os.getenv("RATE_LIMITED_KEY_DETAIL_PREFIX", "logs/key_429_detail_")
    KEYS_SEND_DETAIL_PREFIX = os.getenv("KEYS_SEND_DETAIL_PREFIX", "logs/keys_send_detail_")

    # 日期范围过滤器配置 (单位：天)
    DATE_RANGE_DAYS = int(os.getenv("DATE_RANGE_DAYS", "730"))  # 默认730天 (约2年)

    # 查询文件路径配置
    QUERIES_FILE = os.getenv("QUERIES_FILE", "queries.txt")

    # 已扫描SHA文件配置
    SCANNED_SHAS_FILE = os.getenv("SCANNED_SHAS_FILE", "scanned_shas.txt")

    # Gemini模型配置
    HAJIMI_CHECK_MODEL = os.getenv("HAJIMI_CHECK_MODEL", "gemini-2.5-flash")

    # 并发优化配置
    ENABLE_WORK_STEALING = parse_bool(os.getenv("ENABLE_WORK_STEALING", "true"))
    ENABLE_SMART_LOAD_BALANCING = parse_bool(os.getenv("ENABLE_SMART_LOAD_BALANCING", "true"))
    ENABLE_BACKPRESSURE_CONTROL = parse_bool(os.getenv("ENABLE_BACKPRESSURE_CONTROL", "true"))
    ENABLE_PRIORITY_QUEUE = parse_bool(os.getenv("ENABLE_PRIORITY_QUEUE", "true"))

    # Worker数量配置
    MIN_FILE_WORKERS_PER_TOKEN = int(os.getenv("MIN_FILE_WORKERS_PER_TOKEN", "1"))
    MAX_FILE_WORKERS_PER_TOKEN = int(os.getenv("MAX_FILE_WORKERS_PER_TOKEN", "3"))
    MIN_VALIDATION_WORKERS = int(os.getenv("MIN_VALIDATION_WORKERS", "2"))
    MAX_VALIDATION_WORKERS = int(os.getenv("MAX_VALIDATION_WORKERS", "8"))

    # 队列大小配置
    FILE_QUEUE_SIZE = int(os.getenv("FILE_QUEUE_SIZE", "200"))
    KEY_QUEUE_SIZE = int(os.getenv("KEY_QUEUE_SIZE", "100"))

    # 背压控制配置
    GITHUB_QUOTA_WARNING_THRESHOLD = int(os.getenv("GITHUB_QUOTA_WARNING_THRESHOLD", "30"))
    GITHUB_QUOTA_CRITICAL_THRESHOLD = int(os.getenv("GITHUB_QUOTA_CRITICAL_THRESHOLD", "10"))
    RATE_LIMIT_BACKOFF_MULTIPLIER = float(os.getenv("RATE_LIMIT_BACKOFF_MULTIPLIER", "2.0"))

    # 文件路径黑名单配置
    FILE_PATH_BLACKLIST_STR = os.getenv("FILE_PATH_BLACKLIST", "readme,docs,doc/,.md,sample,tutorial")
    FILE_PATH_BLACKLIST = [token.strip().lower() for token in FILE_PATH_BLACKLIST_STR.split(",") if token.strip()]

    @classmethod
    def get_random_proxy(cls) -> dict[str, str] | None:
        """
        随机获取一个代理配置

        Returns:
            Optional[Dict[str, str]]: requests格式的proxies字典，如果未配置则返回None
        """
        if not cls.PROXY_LIST:
            return None

        # 随机选择一个代理
        proxy_url = random.choice(cls.PROXY_LIST).strip()

        # 返回requests格式的proxies字典
        return {"http": proxy_url, "https": proxy_url}

    @classmethod
    def check(cls) -> bool:
        """
        检查必要的配置是否完整

        Returns:
            bool: 配置是否完整
        """
        logger.info("🔍 Checking required configurations...")

        errors = []

        # 检查GitHub tokens
        if not cls.GITHUB_TOKENS:
            errors.append("GitHub tokens not found. Please set GITHUB_TOKENS environment variable.")
            logger.error("❌ GitHub tokens: Missing")
        else:
            logger.info(f"✅ GitHub tokens: {len(cls.GITHUB_TOKENS)} configured")

        # 检查Gemini Balancer配置
        if cls.GEMINI_BALANCER_SYNC_ENABLED:
            logger.info(f"✅ Gemini Balancer enabled, URL: {cls.GEMINI_BALANCER_URL}")
            if not cls.GEMINI_BALANCER_AUTH or not cls.GEMINI_BALANCER_URL:
                logger.warning("⚠️ Gemini Balancer Auth or URL Missing (Balancer功能将被禁用)")
            else:
                logger.info("✅ Gemini Balancer Auth: ****")
        else:
            logger.info("ℹ️ Gemini Balancer URL: Not configured (Balancer功能将被禁用)")

        # 检查GPT Load Balancer配置
        if parse_bool(cls.GPT_LOAD_SYNC_ENABLED):
            logger.info(f"✅ GPT Load Balancer enabled, URL: {cls.GPT_LOAD_URL}")
            if not cls.GPT_LOAD_AUTH or not cls.GPT_LOAD_URL or not cls.GPT_LOAD_GROUP_NAME:
                logger.warning("⚠️ GPT Load Balancer Auth, URL or Group Name Missing (Load Balancer功能将被禁用)")
            else:
                logger.info("✅ GPT Load Balancer Auth: ****")
                logger.info(f"✅ GPT Load Balancer Group Name: {cls.GPT_LOAD_GROUP_NAME}")
        else:
            logger.info("ℹ️ GPT Load Balancer: Not configured (Load Balancer功能将被禁用)")

        if errors:
            logger.error("❌ Configuration check failed:")
            logger.info("Please check your .env file and configuration.")
            return False

        logger.info("✅ All required configurations are valid")
        return True


logger.info("*" * 30 + " CONFIG START " + "*" * 30)
logger.info(f"GITHUB_TOKENS: {len(Config.GITHUB_TOKENS)} tokens")
logger.info(f"DATA_PATH: {Config.DATA_PATH}")
logger.info(f"PROXY_LIST: {len(Config.PROXY_LIST)} proxies configured")
logger.info(f"GEMINI_BALANCER_URL: {Config.GEMINI_BALANCER_URL or 'Not configured'}")
logger.info(f"GEMINI_BALANCER_AUTH: {'Configured' if Config.GEMINI_BALANCER_AUTH else 'Not configured'}")
logger.info(f"GEMINI_BALANCER_SYNC_ENABLED: {parse_bool(Config.GEMINI_BALANCER_SYNC_ENABLED)}")
logger.info(f"GPT_LOAD_SYNC_ENABLED: {parse_bool(Config.GPT_LOAD_SYNC_ENABLED)}")
logger.info(f"GPT_LOAD_URL: {Config.GPT_LOAD_URL or 'Not configured'}")
logger.info(f"GPT_LOAD_AUTH: {'Configured' if Config.GPT_LOAD_AUTH else 'Not configured'}")
logger.info(f"GPT_LOAD_GROUP_NAME: {Config.GPT_LOAD_GROUP_NAME or 'Not configured'}")
logger.info(f"VALID_KEY_PREFIX: {Config.VALID_KEY_PREFIX}")
logger.info(f"RATE_LIMITED_KEY_PREFIX: {Config.RATE_LIMITED_KEY_PREFIX}")
logger.info(f"KEYS_SEND_PREFIX: {Config.KEYS_SEND_PREFIX}")
logger.info(f"VALID_KEY_DETAIL_PREFIX: {Config.VALID_KEY_DETAIL_PREFIX}")
logger.info(f"RATE_LIMITED_KEY_DETAIL_PREFIX: {Config.RATE_LIMITED_KEY_DETAIL_PREFIX}")
logger.info(f"KEYS_SEND_DETAIL_PREFIX: {Config.KEYS_SEND_DETAIL_PREFIX}")
logger.info(f"DATE_RANGE_DAYS: {Config.DATE_RANGE_DAYS} days")
logger.info(f"QUERIES_FILE: {Config.QUERIES_FILE}")
logger.info(f"SCANNED_SHAS_FILE: {Config.SCANNED_SHAS_FILE}")
logger.info(f"HAJIMI_CHECK_MODEL: {Config.HAJIMI_CHECK_MODEL}")
logger.info(f"FILE_PATH_BLACKLIST: {len(Config.FILE_PATH_BLACKLIST)} items")
logger.info("--- CONCURRENCY OPTIMIZATIONS ---")
logger.info(f"ENABLE_WORK_STEALING: {Config.ENABLE_WORK_STEALING}")
logger.info(f"ENABLE_SMART_LOAD_BALANCING: {Config.ENABLE_SMART_LOAD_BALANCING}")
logger.info(f"ENABLE_BACKPRESSURE_CONTROL: {Config.ENABLE_BACKPRESSURE_CONTROL}")
logger.info(f"ENABLE_PRIORITY_QUEUE: {Config.ENABLE_PRIORITY_QUEUE}")
logger.info(f"FILE_WORKERS_RANGE: {Config.MIN_FILE_WORKERS_PER_TOKEN}-{Config.MAX_FILE_WORKERS_PER_TOKEN} per token")
logger.info(f"VALIDATION_WORKERS_RANGE: {Config.MIN_VALIDATION_WORKERS}-{Config.MAX_VALIDATION_WORKERS}")
logger.info(f"QUEUE_SIZES: File={Config.FILE_QUEUE_SIZE}, Key={Config.KEY_QUEUE_SIZE}")
logger.info(
    f"QUOTA_THRESHOLDS: Warning={Config.GITHUB_QUOTA_WARNING_THRESHOLD}%, Critical={Config.GITHUB_QUOTA_CRITICAL_THRESHOLD}%"
)
logger.info("*" * 30 + " CONFIG END " + "*" * 30)

# 创建全局配置实例
config = Config()
