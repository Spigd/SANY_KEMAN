#!/usr/bin/env python3
"""
快速测试脚本 - 验证系统功能 V4
"""

import sys
import os
import asyncio
import json
from pathlib import Path

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from search.hybrid_searcher import HybridSearcher
from core.models import SearchRequest
from core.config import config
from indexing.data_loader import MetadataLoader


async def test_system():
    """测试系统功能"""
    print("🚀 元数据搜索系统 V4 - 快速测试")
    print("=" * 60)
    
    try:
        # 1. 测试数据加载
        print("\n📊 步骤1: 测试数据加载...")
        loader = MetadataLoader()
        fields = loader.load_from_excel()
        
        if not fields:
            print("❌ 数据加载失败：未找到有效字段")
            return False
        
        print(f"✅ 成功加载 {len(fields)} 个字段")
        
        # 统计字段类型
        dimension_fields = [f for f in fields if f.field_type == 'dimension']
        metric_fields = [f for f in fields if f.field_type == 'metric']
        
        print(f"   - 维度字段: {len(dimension_fields)} 个")
        print(f"   - 指标字段: {len(metric_fields)} 个")
        
        # 显示示例字段
        if dimension_fields:
            print(f"   - 维度字段示例: {dimension_fields[0].display_name} ({dimension_fields[0].table_name}.{dimension_fields[0].column_name})")
        if metric_fields:
            print(f"   - 指标字段示例: {metric_fields[0].display_name} ({metric_fields[0].table_name}.{metric_fields[0].column_name})")
        
        # 2. 测试数据库连接（如果配置了）
        print("\n🗄️ 步骤2: 测试数据库连接...")
        if config.DATABASE_CONFIGS:
            try:
                from indexing.dimension_extractor import EnhancedDimensionExtractor
                
                extractor = EnhancedDimensionExtractor()
                connection_results = extractor.test_connections()
                
                if connection_results:
                    print(f"✅ 配置了 {len(connection_results)} 个数据源")
                    for name, result in connection_results.items():
                        status = "✅ 连接成功" if result.get('connected') else "❌ 连接失败"
                        print(f"   - {name} ({result.get('type', 'unknown')}): {status}")
                        if not result.get('connected') and 'error' in result:
                            print(f"     错误: {result['error']}")
                else:
                    print("❌ 没有可用的数据库连接")
                
                extractor.close_connections()
            except Exception as e:
                print(f"❌ 数据库连接测试失败: {e}")
        else:
            print("⚠️ 未配置数据库连接，跳过数据库测试")
        
        # 3. 测试搜索引擎初始化
        print("\n🔍 步骤3: 初始化搜索引擎...")
        searcher = HybridSearcher()
        
        # 创建索引并加载数据
        result = searcher.create_index_with_data(force_recreate=False)
        
        if result.get('success'):
            print(f"✅ 搜索引擎初始化成功")
            print(f"   - 消息: {result.get('message', '')}")
            print(f"   - 耗时: {result.get('took', 0)}ms")
            
            # 显示统计信息
            stats = result.get('stats', {})
            if 'engines' in stats:
                engines = stats['engines']
                print(f"   - 可用引擎: {list(engines.keys())}")
            
            # 显示维度索引统计
            dimension_stats = stats.get('dimension_indexing', {})
            if dimension_stats:
                if 'dimension_values_indexed' in dimension_stats:
                    print(f"   - 维度值索引: {dimension_stats['dimension_values_indexed']} 个值")
                if 'error' in dimension_stats:
                    print(f"   - 维度值索引错误: {dimension_stats['error']}")
        else:
            print(f"❌ 搜索引擎初始化失败: {result.get('message', '')}")
            return False
        
        # 4. 测试字段搜索
        print("\n🔎 步骤4: 测试字段搜索...")
        
        test_queries = [
            ("客户", "hybrid"),
            ("编码", "elasticsearch"),
            ("状态", "ac_matcher")
        ]
        
        for query, method in test_queries:
            try:
                request = SearchRequest(
                    query=query,
                    search_method=method,
                    size=3,
                    use_tokenization=True
                )
                
                response = searcher.search(request)
                
                if response.total > 0:
                    print(f"✅ {method} 搜索 '{query}': 找到 {response.total} 个结果 ({response.took}ms)")
                    for i, result in enumerate(response.results[:2]):
                        print(f"   {i+1}. {result.field.display_name} ({result.field.table_name}.{result.field.column_name}) - 分数: {result.score:.2f}")
                else:
                    print(f"⚠️ {method} 搜索 '{query}': 未找到结果")
                    
            except Exception as e:
                print(f"❌ {method} 搜索失败: {e}")
        
        # 5. 测试维度值搜索（如果启用了维度值索引）
        print("\n🎯 步骤5: 测试维度值搜索...")
        
        if config.is_dimension_indexing_enabled() and searcher.es_engine and searcher.es_engine.dimension_values_index_exists():
            test_dimension_queries = ["完成", "北京", "VIP"]
            
            for query in test_dimension_queries:
                try:
                    request = SearchRequest(
                        query=query,
                        search_method="dimension_values",
                        size=3,
                        use_tokenization=False
                    )
                    
                    response = searcher.search(request)
                    
                    if response.total > 0:
                        print(f"✅ 维度值搜索 '{query}': 找到 {response.total} 个结果 ({response.took}ms)")
                        for i, result in enumerate(response.results[:2]):
                            dimension_value = result.extra_info.get('dimension_value', '')
                            frequency = result.extra_info.get('frequency', 0)
                            print(f"   {i+1}. {result.field.display_name}: '{dimension_value}' (频次: {frequency})")
                    else:
                        print(f"⚠️ 维度值搜索 '{query}': 未找到结果")
                        
                except Exception as e:
                    print(f"❌ 维度值搜索失败: {e}")
        else:
            print("⚠️ 维度值索引未启用或不存在，跳过维度值搜索测试")
        
        # 6. 测试分词功能
        print("\n🔤 步骤6: 测试分词功能...")
        
        if searcher.es_engine:
            try:
                tokenization_result = searcher.es_engine.tokenize_text(
                    "我想查询客户的订单状态信息", 
                    "ik_max_word"
                )
                
                print(f"✅ 分词测试成功 ({tokenization_result.took}ms)")
                print(f"   - 原文: {tokenization_result.original_text}")
                print(f"   - 分词器: {tokenization_result.tokenizer_type}")
                print(f"   - 分词结果: {tokenization_result.tokens[:8]}...")  # 只显示前8个词
                
            except Exception as e:
                print(f"❌ 分词测试失败: {e}")
        
        # 7. 获取系统统计
        print("\n📈 步骤7: 系统统计信息...")
        
        try:
            stats = searcher.get_stats()
            print(f"✅ 系统统计:")
            print(f"   - 总字段数: {stats.get('total_fields', 0)}")
            print(f"   - 搜索器初始化状态: {stats.get('initialized', False)}")
            
            engines = stats.get('engines', {})
            for engine_name, engine_stats in engines.items():
                available = engine_stats.get('available', False)
                status = "✅ 可用" if available else "❌ 不可用"
                print(f"   - {engine_name}: {status}")
                
        except Exception as e:
            print(f"❌ 获取统计信息失败: {e}")
        
        print("\n" + "=" * 60)
        print("🎉 系统测试完成！")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 系统测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_api_endpoints():
    """测试API端点（需要服务运行）"""
    print("\n🌐 API端点测试...")
    
    try:
        import httpx
        
        base_url = f"http://localhost:{config.API_PORT}/api/search"
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # 测试健康检查
            try:
                response = await client.get(f"{base_url}/health")
                if response.status_code == 200:
                    print("✅ 健康检查: API服务正常运行")
                else:
                    print(f"⚠️ 健康检查: HTTP {response.status_code}")
            except Exception as e:
                print(f"❌ API服务未运行或不可访问: {e}")
                return False
            
            # 测试字段搜索
            try:
                response = await client.get(f"{base_url}/fields?q=客户&size=3")
                if response.status_code == 200:
                    data = response.json()
                    print(f"✅ 字段搜索API: 找到 {data.get('total', 0)} 个结果")
                else:
                    print(f"❌ 字段搜索API: HTTP {response.status_code}")
            except Exception as e:
                print(f"❌ 字段搜索API失败: {e}")
            
            # 测试维度值搜索
            try:
                response = await client.get(f"{base_url}/dimension-values?q=完成&size=3")
                if response.status_code == 200:
                    data = response.json()
                    print(f"✅ 维度值搜索API: 找到 {data.get('total', 0)} 个结果")
                else:
                    print(f"❌ 维度值搜索API: HTTP {response.status_code}")
            except Exception as e:
                print(f"❌ 维度值搜索API失败: {e}")
            
            # 测试数据库连接
            try:
                response = await client.get(f"{base_url}/database/test")
                if response.status_code == 200:
                    data = response.json()
                    if data.get('success'):
                        healthy_count = data.get('healthy_connections', 0)
                        total_count = data.get('total_connections', 0)
                        print(f"✅ 数据库连接测试: {healthy_count}/{total_count} 连接健康")
                    else:
                        print("❌ 数据库连接测试失败")
                else:
                    print(f"❌ 数据库连接测试: HTTP {response.status_code}")
            except Exception as e:
                print(f"❌ 数据库连接测试失败: {e}")
        
        return True
        
    except ImportError:
        print("⚠️ httpx未安装，跳过API测试")
        return True
    except Exception as e:
        print(f"❌ API测试失败: {e}")
        return False


async def main():
    """主函数"""
    print("🚀 启动元数据搜索系统 V4 快速测试")
    
    # # 基础功能测试
    # basic_test_result = await test_system()
    
    # if not basic_test_result:
    #     print("\n❌ 基础测试失败，请检查配置和数据文件")
    #     return
    
    # API测试（可选）
    print("\n" + "=" * 60)
    api_test_result = await test_api_endpoints()
    
    print("\n" + "=" * 60)
    # if basic_test_result and api_test_result:
    #     print("🎉 所有测试通过！系统运行正常")
        
    #     print("\n📋 下一步操作建议:")
    #     print("1. 访问 API 文档: http://localhost:8082/docs")
    #     print("2. 测试字段搜索: curl 'http://localhost:8082/api/search/fields?q=客户'")
    #     print("3. 测试维度值搜索: curl 'http://localhost:8082/api/search/dimension-values?q=完成'")
    #     print("4. 查看系统状态: curl 'http://localhost:8082/api/search/stats'")
    # else:
    #     print("⚠️ 部分测试失败，请检查日志和配置")


if __name__ == "__main__":
    asyncio.run(main()) 