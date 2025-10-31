"""
测试Metric（指标）检索系统
"""

import logging
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from indexing.data_loader import MetricLoader
from search.elasticsearch_engine import ElasticsearchEngine
from search.hybrid_searcher import HybridSearcher
from core.models import MetricSearchRequest

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_metric_loader():
    """测试Metric数据加载器"""
    logger.info("=" * 60)
    logger.info("测试1: Metric数据加载器")
    logger.info("=" * 60)
    
    try:
        loader = MetricLoader()
        metrics = loader.load_from_excel()
        
        logger.info(f"✅ 成功加载 {len(metrics)} 个指标")
        
        if metrics:
            # 显示第一个指标
            first_metric = metrics[0]
            logger.info(f"\n示例指标:")
            logger.info(f"  ID: {first_metric.metric_id}")
            logger.info(f"  名称: {first_metric.metric_name}")
            logger.info(f"  别名: {first_metric.metric_alias}")
            logger.info(f"  类型: {first_metric.metric_type}")
            logger.info(f"  状态: {first_metric.status}")
            logger.info(f"  相关实体: {first_metric.related_entities[:3] if len(first_metric.related_entities) > 3 else first_metric.related_entities}")
        
        # 验证数据
        stats = loader.validate_metrics(metrics)
        logger.info(f"\n数据验证:")
        logger.info(f"  总计: {stats['total']}")
        logger.info(f"  有效: {stats['valid']}")
        logger.info(f"  无效: {stats['invalid']}")
        logger.info(f"  活跃: {stats['active_metrics']}")
        logger.info(f"  非活跃: {stats['inactive_metrics']}")
        logger.info(f"  指标类型分布: {stats['metric_types']}")
        
        return True, metrics
        
    except Exception as e:
        logger.error(f"❌ Metric数据加载测试失败: {e}")
        return False, []


def test_metric_index_creation(metrics):
    """测试Metric索引创建"""
    logger.info("\n" + "=" * 60)
    logger.info("测试2: Metric索引创建")
    logger.info("=" * 60)
    
    try:
        es_engine = ElasticsearchEngine()
        
        # 创建索引
        logger.info("创建Metric索引...")
        result = es_engine.create_metric_index(force=True)
        
        if result:
            logger.info("✅ Metric索引创建成功")
            
            # 索引数据
            logger.info(f"索引 {len(metrics)} 个指标...")
            index_result = es_engine.index_metrics(metrics)
            
            if index_result:
                logger.info("✅ Metric数据索引成功")
                return True
            else:
                logger.error("❌ Metric数据索引失败")
                return False
        else:
            logger.error("❌ Metric索引创建失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ Metric索引创建测试失败: {e}")
        return False


def test_metric_search():
    """测试Metric搜索"""
    logger.info("\n" + "=" * 60)
    logger.info("测试3: Metric搜索功能")
    logger.info("=" * 60)
    
    try:
        searcher = HybridSearcher()
        
        # 测试搜索1: 按名称搜索
        logger.info("\n搜索测试1: 搜索'拜访'")
        request = MetricSearchRequest(
            query="拜访",
            size=5
        )
        response = searcher.search_metrics(request)
        logger.info(f"找到 {response.total} 个结果, 耗时 {response.took}ms")
        
        if response.results:
            for i, result in enumerate(response.results[:3], 1):
                logger.info(f"\n  结果 {i}:")
                logger.info(f"    名称: {result.metric.metric_name}")
                logger.info(f"    类型: {result.metric.metric_type}")
                logger.info(f"    分数: {result.score:.2f}")
                logger.info(f"    匹配: {result.matched_text}")
        
        # 测试搜索2: 按类型过滤
        logger.info("\n搜索测试2: 搜索类型为'count'的指标")
        request2 = MetricSearchRequest(
            query="客户",
            metric_type="count",
            size=3
        )
        response2 = searcher.search_metrics(request2)
        logger.info(f"找到 {response2.total} 个结果")
        
        if response2.results:
            for i, result in enumerate(response2.results, 1):
                logger.info(f"  {i}. {result.metric.metric_name} ({result.metric.metric_type})")
        
        # 测试搜索3: 按状态过滤
        logger.info("\n搜索测试3: 搜索活跃状态的指标")
        request3 = MetricSearchRequest(
            query="数",
            status="active",
            size=5
        )
        response3 = searcher.search_metrics(request3)
        logger.info(f"找到 {response3.total} 个活跃指标")
        
        logger.info("\n✅ Metric搜索测试完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ Metric搜索测试失败: {e}")
        return False


def main():
    """主测试函数"""
    logger.info("开始测试Metric检索系统")
    logger.info("=" * 60)
    
    # 测试1: 数据加载
    success1, metrics = test_metric_loader()
    if not success1 or not metrics:
        logger.error("数据加载失败，终止测试")
        return False
    
    # 测试2: 索引创建
    success2 = test_metric_index_creation(metrics)
    if not success2:
        logger.error("索引创建失败，终止测试")
        return False
    
    # 测试3: 搜索功能
    success3 = test_metric_search()
    
    # 总结
    logger.info("\n" + "=" * 60)
    logger.info("测试总结")
    logger.info("=" * 60)
    logger.info(f"数据加载: {'✅ 通过' if success1 else '❌ 失败'}")
    logger.info(f"索引创建: {'✅ 通过' if success2 else '❌ 失败'}")
    logger.info(f"搜索功能: {'✅ 通过' if success3 else '❌ 失败'}")
    
    all_success = success1 and success2 and success3
    logger.info(f"\n总体结果: {'✅ 全部通过' if all_success else '❌ 部分失败'}")
    
    return all_success


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"测试过程中发生错误: {e}", exc_info=True)
        sys.exit(1)

