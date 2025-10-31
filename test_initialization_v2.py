#!/usr/bin/env python3
"""
测试初始化逻辑 V2 - 验证修复后的重复初始化问题
"""

import sys
import os
import logging
import time

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from search.hybrid_searcher import HybridSearcher
from search.elasticsearch_engine import ElasticsearchEngine

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def clear_indices():
    """清理所有索引（用于测试）"""
    logger.info("=== 清理现有索引（用于测试） ===")
    try:
        engine = ElasticsearchEngine()
        
        # 删除主字段索引
        if engine.index_exists():
            engine.es.indices.delete(index=engine.fields_index_name)
            logger.info(f"已删除主字段索引: {engine.fields_index_name}")
        
        # 删除维度值索引
        if engine.dimension_values_index_exists():
            engine.es.indices.delete(index=engine.dimension_values_index_name)
            logger.info(f"已删除维度值索引: {engine.dimension_values_index_name}")
        
        time.sleep(2)  # 等待删除操作完成
        return True
    except Exception as e:
        logger.error(f"清理索引失败: {e}")
        return False

def test_first_initialization():
    """测试首次初始化"""
    logger.info("=== 测试首次初始化 ===")
    
    start_time = time.time()
    searcher = HybridSearcher()
    result = searcher.create_index_with_data(force_recreate=False)
    end_time = time.time()
    
    duration = (end_time - start_time) * 1000
    
    logger.info(f"首次初始化结果: {result.get('success')}")
    logger.info(f"首次初始化消息: {result.get('message')}")
    logger.info(f"首次初始化耗时: {duration:.0f}ms (函数返回: {result.get('took', 0)}ms)")
    
    return result, duration

def test_second_initialization():
    """测试第二次初始化（应该跳过）"""
    logger.info("=== 测试第二次初始化（应该跳过） ===")
    
    start_time = time.time()
    searcher = HybridSearcher()
    result = searcher.create_index_with_data(force_recreate=False)
    end_time = time.time()
    
    duration = (end_time - start_time) * 1000
    
    logger.info(f"第二次初始化结果: {result.get('success')}")
    logger.info(f"第二次初始化消息: {result.get('message')}")
    logger.info(f"第二次初始化耗时: {duration:.0f}ms (函数返回: {result.get('took', 0)}ms)")
    
    return result, duration

def test_api_simulation():
    """模拟API调用场景"""
    logger.info("=== 模拟API调用场景 ===")
    
    # 模拟多次API调用
    times = []
    for i in range(3):
        logger.info(f"第 {i+1} 次API调用...")
        
        start_time = time.time()
        searcher = HybridSearcher()
        
        # 模拟 get_hybrid_searcher 的逻辑
        if not searcher.initialized:
            result = searcher.create_index_with_data(force_recreate=False)
            logger.info(f"第 {i+1} 次调用初始化结果: {result.get('message', '')}")
        else:
            logger.info(f"第 {i+1} 次调用: 搜索器已初始化，跳过")
        
        end_time = time.time()
        duration = (end_time - start_time) * 1000
        times.append(duration)
        
        logger.info(f"第 {i+1} 次调用耗时: {duration:.0f}ms")
        time.sleep(0.5)  # 短暂间隔
    
    return times

def check_index_status():
    """检查索引状态"""
    logger.info("=== 检查索引状态 ===")
    
    try:
        engine = ElasticsearchEngine()
        
        # 检查主字段索引
        fields_exists = engine.index_exists()
        fields_count = 0
        if fields_exists:
            count_response = engine.es.count(index=engine.fields_index_name)
            fields_count = count_response.get('count', 0)
        
        # 检查维度值索引
        dimensions_exists = engine.dimension_values_index_exists()
        dimensions_count = 0
        if dimensions_exists:
            count_response = engine.es.count(index=engine.dimension_values_index_name)
            dimensions_count = count_response.get('count', 0)
        
        logger.info(f"主字段索引: 存在={fields_exists}, 数据量={fields_count}")
        logger.info(f"维度值索引: 存在={dimensions_exists}, 数据量={dimensions_count}")
        
        return {
            'fields_exists': fields_exists,
            'fields_count': fields_count,
            'dimensions_exists': dimensions_exists,
            'dimensions_count': dimensions_count
        }
    
    except Exception as e:
        logger.error(f"检查索引状态失败: {e}")
        return None

def main():
    """主测试函数"""
    logger.info("开始测试修复后的初始化逻辑...")
    
    try:
        # 步骤1: 清理现有索引
        if not clear_indices():
            logger.error("清理索引失败，测试终止")
            return
        
        # 步骤2: 检查初始状态
        logger.info("检查清理后的索引状态...")
        initial_status = check_index_status()
        
        # 步骤3: 首次初始化
        result1, duration1 = test_first_initialization()
        
        # 步骤4: 检查首次初始化后的状态
        after_first_status = check_index_status()
        
        # 步骤5: 第二次初始化（应该跳过）
        result2, duration2 = test_second_initialization()
        
        # 步骤6: API调用模拟
        api_times = test_api_simulation()
        
        # 步骤7: 最终检查
        final_status = check_index_status()
        
        # 结果分析
        logger.info("=== 测试结果分析 ===")
        
        # 时间分析
        logger.info(f"首次初始化耗时: {duration1:.0f}ms")
        logger.info(f"第二次初始化耗时: {duration2:.0f}ms")
        logger.info(f"API调用耗时: {[f'{t:.0f}ms' for t in api_times]}")
        
        # 性能改进检查
        if duration2 < duration1 * 0.1:
            logger.info("✅ 第二次初始化成功跳过，性能显著提升")
        elif duration2 < duration1 * 0.5:
            logger.info("⚠️  第二次初始化有所改进，但可能仍有优化空间")
        else:
            logger.warning("❌ 第二次初始化耗时仍然很长，可能存在问题")
        
        # API调用性能检查
        avg_api_time = sum(api_times) / len(api_times)
        if avg_api_time < 1000:  # 小于1秒
            logger.info("✅ API调用响应时间优秀")
        elif avg_api_time < 3000:  # 小于3秒
            logger.info("⚠️  API调用响应时间可接受")
        else:
            logger.warning("❌ API调用响应时间过长")
        
        # 数据一致性检查
        if (after_first_status and final_status and
            after_first_status['fields_count'] == final_status['fields_count'] and
            after_first_status['dimensions_count'] == final_status['dimensions_count']):
            logger.info("✅ 数据一致性检查通过")
        else:
            logger.warning("⚠️  数据一致性检查异常")
        
        # 总体评估
        if (duration2 < duration1 * 0.1 and avg_api_time < 1000):
            logger.info("🎉 测试通过！重复初始化问题已解决")
        else:
            logger.warning("⚠️  测试结果不理想，可能需要进一步优化")
        
    except Exception as e:
        logger.error(f"测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 