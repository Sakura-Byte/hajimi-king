import asyncio
import sys
import traceback
from datetime import datetime

from common.Logger import logger

sys.path.append("../")
from common.config import Config
from utils.async_processor_optimized import OptimizedAsyncProcessor
from utils.file_manager import checkpoint, file_manager
from utils.github_client import GitHubClient
from utils.sync_utils import sync_utils

# 创建GitHub工具实例和文件管理器
github_utils = GitHubClient.create_instance(Config.GITHUB_TOKENS)


def normalize_query(query: str) -> str:
    query = " ".join(query.split())

    parts = []
    i = 0
    while i < len(query):
        if query[i] == '"':
            end_quote = query.find('"', i + 1)
            if end_quote != -1:
                parts.append(query[i : end_quote + 1])
                i = end_quote + 1
            else:
                parts.append(query[i])
                i += 1
        elif query[i] == " ":
            i += 1
        else:
            start = i
            while i < len(query) and query[i] != " ":
                i += 1
            parts.append(query[start:i])

    quoted_strings = []
    language_parts = []
    filename_parts = []
    path_parts = []
    other_parts = []

    for part in parts:
        if part.startswith('"') and part.endswith('"'):
            quoted_strings.append(part)
        elif part.startswith("language:"):
            language_parts.append(part)
        elif part.startswith("filename:"):
            filename_parts.append(part)
        elif part.startswith("path:"):
            path_parts.append(part)
        elif part.strip():
            other_parts.append(part)

    normalized_parts = []
    normalized_parts.extend(sorted(quoted_strings))
    normalized_parts.extend(sorted(other_parts))
    normalized_parts.extend(sorted(language_parts))
    normalized_parts.extend(sorted(filename_parts))
    normalized_parts.extend(sorted(path_parts))

    return " ".join(normalized_parts)


async def async_main():
    """异步主函数 - 支持并发处理"""
    start_time = datetime.now()

    # 打印系统启动信息
    logger.info("=" * 60)
    logger.info("🚀 HAJIMI KING STARTING (ASYNC MODE)")
    logger.info("=" * 60)
    logger.info(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. 检查配置
    if not Config.check():
        logger.info("❌ Config check failed. Exiting...")
        sys.exit(1)
    # 2. 检查文件管理器
    if not file_manager.check():
        logger.error("❌ FileManager check failed. Exiting...")
        sys.exit(1)

    # 2.5. 显示SyncUtils状态和队列信息
    if sync_utils.balancer_enabled:
        logger.info("🔗 SyncUtils ready for async key syncing")

    # 显示队列状态
    balancer_queue_count = len(checkpoint.wait_send_balancer)
    gpt_load_queue_count = len(checkpoint.wait_send_gpt_load)
    logger.info(f"📊 Queue status - Balancer: {balancer_queue_count}, GPT Load: {gpt_load_queue_count}")

    # 3. 显示系统信息
    search_queries = file_manager.get_search_queries()
    logger.info("📋 SYSTEM INFORMATION:")
    logger.info(f"🔑 GitHub tokens: {len(Config.GITHUB_TOKENS)} configured")
    logger.info(f"🔍 Search queries: {len(search_queries)} loaded")
    logger.info(f"📅 Date filter: {Config.DATE_RANGE_DAYS} days")
    if Config.PROXY_LIST:
        logger.info(f"🌐 Proxy: {len(Config.PROXY_LIST)} proxies configured")

    if checkpoint.last_scan_time:
        logger.info("💾 Checkpoint found - Incremental scan mode")
        logger.info(f"   Last scan: {checkpoint.last_scan_time}")
        logger.info(f"   Scanned files: {len(checkpoint.scanned_shas)}")
        logger.info(f"   Processed queries: {len(checkpoint.processed_queries)}")
    else:
        logger.info("💾 No checkpoint - Full scan mode")

    # 4. 创建并启动优化的异步处理器
    processor = OptimizedAsyncProcessor(
        max_file_workers=len(Config.GITHUB_TOKENS),  # 限制为token数量，1个token对应1个worker
        max_validation_workers=Config.MAX_VALIDATION_WORKERS,
        file_queue_size=Config.FILE_QUEUE_SIZE,
        key_queue_size=Config.KEY_QUEUE_SIZE,
        enable_work_stealing=Config.ENABLE_WORK_STEALING,
        enable_smart_load_balancing=Config.ENABLE_SMART_LOAD_BALANCING,
    )

    await processor.start()

    logger.info("✅ System ready - Starting concurrent processing")
    logger.info("=" * 60)

    total_queries_processed = 0
    loop_count = 0

    try:
        while True:
            loop_count += 1
            logger.info(f"🔄 Loop #{loop_count} - {datetime.now().strftime('%H:%M:%S')}")

            query_count = 0
            loop_tasks_added = 0

            for i, q in enumerate(search_queries, 1):
                normalized_q = normalize_query(q)
                if normalized_q in checkpoint.processed_queries:
                    logger.info(f"🔍 Skipping already processed query: [{q}], index:#{i}")
                    continue

                # GitHub搜索
                res = github_utils.search_for_keys(q)

                if res and "items" in res:
                    items = res["items"]
                    if items:
                        logger.info(f"🔍 Query {i}/{len(search_queries)}: 【{q}】 found {len(items)} items")

                        # 将所有items添加到处理队列
                        tasks_added = 0
                        for item in items:
                            # 记录SHA到checkpoint (立即记录避免重复处理)
                            checkpoint.add_scanned_sha(item.get("sha"))

                            # 添加到异步处理队列
                            success = await processor.add_file_task(item, q)
                            if success:
                                tasks_added += 1

                        loop_tasks_added += tasks_added
                        logger.info(f"📥 Added {tasks_added}/{len(items)} tasks to processing queue")
                    else:
                        logger.info(f"📭 Query {i}/{len(search_queries)} - No items found")

                    # Only mark query as processed when search succeeds
                    checkpoint.add_processed_query(normalized_q)
                    query_count += 1
                    total_queries_processed += 1
                else:
                    logger.warning(f"❌ Query {i}/{len(search_queries)} failed - will retry in next loop")
                    # Don't mark as processed, don't increment counters

                # 定期保存checkpoint
                if query_count % 5 == 0:
                    checkpoint.update_scan_time()
                    file_manager.save_checkpoint(checkpoint)
                    file_manager.update_dynamic_filenames()

                    # 显示处理统计
                    stats = processor.get_optimized_stats()
                    queue_status = processor.get_queue_status()
                    logger.info(
                        f"📊 Progress - Queries: {total_queries_processed}, Files: {stats['files_downloaded']}, Keys found: {stats['keys_extracted']}, Valid: {stats['valid_keys']}, Rate limited: {stats['rate_limited_keys']}"
                    )
                    logger.info(
                        f"📦 Queue status - File queue: {queue_status['file_queue']['current_size']}, Key queue: {queue_status['key_queue']['current_size']}"
                    )

                    # 显示优化指标
                    opt_metrics = stats.get("optimization_metrics", {})
                    logger.info(
                        f"🎯 Optimization - Backpressure: {opt_metrics.get('backpressure_level', 'UNKNOWN')}, GitHub health: {opt_metrics.get('github_health_score', 0):.2f}, Workers: F{opt_metrics.get('worker_counts', {}).get('file_workers', 0)}/V{opt_metrics.get('worker_counts', {}).get('validation_workers', 0)}"
                    )

                # 短暂休息避免API限制
                if query_count % 3 == 0:
                    logger.info(f"⏸️ Processed {query_count} queries, taking a break...")
                    await asyncio.sleep(2)

            # 更新checkpoint
            checkpoint.update_scan_time()
            file_manager.save_checkpoint(checkpoint)
            file_manager.update_dynamic_filenames()

            # 显示循环结果
            stats = processor.get_optimized_stats()
            logger.info(f"🏁 Loop #{loop_count} complete - Added {loop_tasks_added} tasks | Total stats:")
            logger.info(f"   📊 Files processed: {stats['files_downloaded']}, Keys found: {stats['keys_extracted']}")
            logger.info(f"   ✅ Valid keys: {stats['valid_keys']}, ⚠️ Rate limited: {stats['rate_limited_keys']}")
            logger.info(f"   ❌ Errors: {stats['errors']}")

            # 等待队列处理完成 (最多等待30秒)
            logger.info("⏳ Waiting for queues to process...")
            wait_time = 0
            while wait_time < 30:
                queue_status = processor.get_queue_status()
                file_queue_size = queue_status["file_queue"]["current_size"]
                key_queue_size = queue_status["key_queue"]["current_size"]
                if file_queue_size == 0 and key_queue_size == 0:
                    break
                await asyncio.sleep(1)
                wait_time += 1

                # 每5秒显示一次队列状态
                if wait_time % 5 == 0:
                    logger.info(f"📦 Still processing - File queue: {file_queue_size}, Key queue: {key_queue_size}")

            logger.info("💤 Sleeping for 10 seconds before next loop...")
            await asyncio.sleep(10)

    except KeyboardInterrupt:
        logger.info("⛔ Interrupted by user")
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        traceback.print_exc()
    finally:
        # 优雅关闭
        logger.info("🛑 Shutting down...")
        await processor.stop()

        checkpoint.update_scan_time()
        file_manager.save_checkpoint(checkpoint)

        # 最终统计
        final_stats = processor.get_optimized_stats()
        logger.info(
            f"📊 Final stats - Valid keys: {final_stats['valid_keys']}, Rate limited: {final_stats['rate_limited_keys']}"
        )

        # 显示优化效果总结
        opt_metrics = final_stats.get("optimization_metrics", {})
        logger.info("🎯 Optimization summary:")
        logger.info(f"   Final backpressure level: {opt_metrics.get('backpressure_level', 'UNKNOWN')}")
        logger.info(f"   GitHub health score: {opt_metrics.get('github_health_score', 0):.2f}")
        logger.info(
            f"   Final worker count: {opt_metrics.get('worker_counts', {}).get('file_workers', 0)} file, {opt_metrics.get('worker_counts', {}).get('validation_workers', 0)} validation"
        )

        # 显示高级统计（如果可用）
        if "work_stealing" in final_stats:
            ws_stats = final_stats["work_stealing"]["steal_stats"]
            logger.info(
                f"   Work stealing: {ws_stats['total_steals_successful']}/{ws_stats['total_steals_attempted']} successful"
            )

        if "load_balancing" in final_stats:
            lb_stats = final_stats["load_balancing"]["global_stats"]
            logger.info(
                f"   Load balancing: {lb_stats['failover_events']} failovers, {lb_stats['load_redistributions']} rebalances"
            )
        logger.info("🔚 Shutting down sync utils...")
        sync_utils.shutdown()


def main():
    """主函数 - 启动异步并发模式"""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
