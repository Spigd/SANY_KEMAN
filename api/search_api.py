"""
搜索API路由 - V3增强版
支持混合检索和分词控制
"""

import logging
import re
from typing import Optional, List
from fastapi import APIRouter, Query, HTTPException, Depends

from core.models import (
    SearchRequest, SearchResponse, IndexRequest, IndexResponse,
    TokenizationResult, HybridSearchConfig,
    MetricSearchRequest, MetricSearchResponse,
    ComprehensiveAnalysisRequest, ComprehensiveAnalysisResponse
)
from search.hybrid_searcher import HybridSearcher
from indexing.data_loader import MetadataLoader

logger = logging.getLogger(__name__)

router = APIRouter()

def remove_time_from_query(query: str) -> str:
    """
    从查询中移除时间部分，保留其他内容
    
    支持移除的时间格式：
    - 2025-10-13
    - 2025-09-01至2025-10-14
    - 2025-09-01到2025-10-14
    - 2025/10/13
    - 2025.10.13
    - 2025年10月13日
    - 2025-10-13 10:30:00
    - 2025-10-13 10:30
    """
    # 清理查询字符串
    original_query = query
    query = query.strip()
    
    # 时间范围正则表达式（优先匹配范围，因为它们更长）
    range_patterns = [
        # 匹配 "2025-09-01 到 2025-09-30" 这种格式
        r'\d{4}-\d{1,2}-\d{1,2}\s*[至到]\s*\d{4}-\d{1,2}-\d{1,2}',
        r'\d{4}/\d{1,2}/\d{1,2}\s*[至到]\s*\d{4}/\d{1,2}/\d{1,2}',
        r'\d{4}\.\d{1,2}\.\d{1,2}\s*[至到]\s*\d{4}\.\d{1,2}\.\d{1,2}',
        r'\d{4}年\d{1,2}月\d{1,2}日\s*[至到]\s*\d{4}年\d{1,2}月\d{1,2}日',
    ]
    
    # 移除时间范围
    for pattern in range_patterns:
        query = re.sub(pattern, '', query)
    
    # 单独的时间格式正则表达式
    time_patterns = [
        # YYYY-MM-DD HH:MM:SS 格式（带时间的要先匹配）
        r'\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}:\d{1,2}',
        r'\d{4}/\d{1,2}/\d{1,2}\s+\d{1,2}:\d{1,2}:\d{1,2}',
        # YYYY-MM-DD HH:MM 格式
        r'\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}',
        r'\d{4}/\d{1,2}/\d{1,2}\s+\d{1,2}:\d{1,2}',
        # YYYY-MM-DD 格式
        r'\d{4}-\d{1,2}-\d{1,2}',
        # YYYY/MM/DD 格式
        r'\d{4}/\d{1,2}/\d{1,2}',
        # YYYY.MM.DD 格式
        r'\d{4}\.\d{1,2}\.\d{1,2}',
        # YYYY年MM月DD日 格式
        r'\d{4}年\d{1,2}月\d{1,2}日',
    ]
    
    # 移除单独的时间格式
    for pattern in time_patterns:
        query = re.sub(pattern, '', query)
    
    # 清理多余的空格
    query = re.sub(r'\s+', ' ', query).strip()
    
    # 如果处理后的查询为空或只剩下很少的字符，返回原查询
    if len(query) < 2:
        return original_query
    
    # 记录时间过滤日志
    if query != original_query:
        logger.info(f"从查询中移除了时间部分: '{original_query}' -> '{query}'")
    
    return query

# 全局混合搜索器实例
_hybrid_searcher = None
_initialization_attempted = False
_data_sync_scheduler = None

def get_hybrid_searcher() -> HybridSearcher:
    """获取混合搜索器实例 - 支持自动初始化和索引创建"""
    global _hybrid_searcher, _initialization_attempted, _data_sync_scheduler
    
    if _hybrid_searcher is None:
        logger.info("创建混合搜索器实例...")
        _hybrid_searcher = HybridSearcher()
    
    # 只在第一次或被显式重置后才尝试初始化
    if not _initialization_attempted:
        _initialization_attempted = True
        logger.info("首次检测到搜索器，开始检查索引状态...")
        
        # 先检查索引是否已经存在且有数据
        need_initialization = True
        if _hybrid_searcher.es_engine:
            try:
                # 检查字段索引
                fields_exist = False
                fields_count = 0
                if _hybrid_searcher.es_engine.index_exists():
                    count_response = _hybrid_searcher.es_engine.es.count(
                        index=_hybrid_searcher.es_engine.fields_index_name
                    ) 
                    fields_count = count_response.get('count', 0)
                    fields_exist = fields_count > 0
                
                # 检查指标索引
                metrics_exist = False
                metrics_count = 0
                if _hybrid_searcher.es_engine.metric_index_exists():
                    count_response = _hybrid_searcher.es_engine.es.count(
                        index=_hybrid_searcher.es_engine.metric_index_name
                    )
                    metrics_count = count_response.get('count', 0)
                    metrics_exist = metrics_count > 0
                
                # 检查维度值索引
                dimension_values_exist = False
                dimension_values_count = 0
                if _hybrid_searcher.es_engine.dimension_values_index_exists():
                    count_response = _hybrid_searcher.es_engine.es.count(
                        index=_hybrid_searcher.es_engine.dimension_values_index_name
                    )
                    dimension_values_count = count_response.get('count', 0)
                    dimension_values_exist = dimension_values_count > 0
                
                # 输出索引状态
                if fields_exist or metrics_exist or dimension_values_exist:
                    status_msg = []
                    if fields_exist:
                        status_msg.append(f"字段索引({fields_count}条)")
                    if metrics_exist:
                        status_msg.append(f"指标索引({metrics_count}条)")
                    if dimension_values_exist:
                        status_msg.append(f"维度值索引({dimension_values_count}条)")
                    logger.info(f"发现已存在的索引: {', '.join(status_msg)}")
                
                # 只有当三个索引都存在且有数据时，才跳过初始化
                if fields_exist and metrics_exist and dimension_values_exist:
                    logger.info("✅ 字段索引、指标索引和维度值索引都已存在且有数据")
                    
                    # 标记搜索器为已初始化
                    _hybrid_searcher.initialized = True
                    
                    # 检查是否需要初始化AC自动机和相似度匹配器
                    need_other_engines = (
                        (_hybrid_searcher.ac_matcher and not _hybrid_searcher.ac_matcher.initialized) or
                        (_hybrid_searcher.similarity_matcher and not _hybrid_searcher.similarity_matcher.initialized)
                    )
                    
                    if need_other_engines:
                        # 当索引已存在时，AC自动机和相似度匹配器将延迟初始化
                        # 避免在启动时重复从API加载数据
                        logger.info("检测到索引已存在，AC自动机和相似度匹配器将在首次搜索或同步时初始化")
                    else:
                        logger.info("✅ AC自动机和相似度匹配器已初始化，跳过")
                    
                    need_initialization = False
                elif fields_exist and metrics_exist and not dimension_values_exist:
                    logger.info("⚠️ 字段和指标索引存在但维度值索引不存在，需要创建维度值索引")
                elif fields_exist and not metrics_exist:
                    logger.info("⚠️ 字段索引存在但指标/维度值索引不存在，需要创建缺失的索引")
                elif not fields_exist and (metrics_exist or dimension_values_exist):
                    logger.info("⚠️ 字段索引不存在但其他索引存在，需要完整重建所有索引")
                else:
                    logger.info("索引不存在或无数据，需要创建索引和加载数据")
            except Exception as e:
                logger.warning(f"检查索引状态时出错: {e}，将尝试初始化")
        
        # 只有在真正需要时才进行完整初始化
        if need_initialization:
            logger.info("开始自动创建索引和加载数据...")
            try:
                result = _hybrid_searcher.create_index_with_data(
                    excel_path=None,
                    force_recreate=False
                )
                
                if result.get('success', False):
                    logger.info(f"✅ 自动创建索引成功: {result.get('message', '')}")
                    logger.info(f"📊 耗时: {result.get('took', 0)}ms")
                    
                    # 打印统计信息
                    stats = result.get('stats', {})
                    if stats:
                        logger.info(f"📈 统计信息:")
                        logger.info(f"  - 总字段数: {stats.get('total_fields', 0)}")
                        logger.info(f"  - 搜索器状态: {'已初始化' if stats.get('initialized', False) else '未初始化'}")
                        
                        engines = stats.get('engines', {})
                        for engine_name, engine_info in engines.items():
                            status = '✅ 可用' if engine_info.get('available', False) else '❌ 不可用'
                            logger.info(f"  - {engine_name}: {status}")
                else:
                    logger.error(f"❌ 自动创建索引失败: {result.get('message', '未知错误')}")
                    
            except Exception as e:
                logger.error(f"❌ 自动创建索引过程中出错: {e}")
    
    # 启动数据同步调度器（如果启用）
    if _data_sync_scheduler is None:
        from core.config import config
        if config.API_SYNC_ENABLED:
            try:
                from indexing.scheduler import DataSyncScheduler
                logger.info("初始化数据同步调度器...")
                _data_sync_scheduler = DataSyncScheduler(_hybrid_searcher)
                _data_sync_scheduler.start()
            except Exception as e:
                logger.error(f"启动数据同步调度器失败: {e}")
    
    return _hybrid_searcher

def ensure_searcher_ready(searcher: HybridSearcher) -> bool:
    """确保搜索器已准备就绪（已初始化且有数据）"""
    if not searcher.initialized:
        logger.warning("搜索器未初始化，检查索引状态...")
        
        # 检查索引是否实际存在且有数据
        if searcher.es_engine:
            try:
                if searcher.es_engine.index_exists():
                    # 检查索引是否有数据
                    count_response = searcher.es_engine.es.count(index=searcher.es_engine.fields_index_name)
                    existing_count = count_response.get('count', 0)
                    
                    if existing_count > 0:
                        logger.info(f"发现索引已存在且有 {existing_count} 条数据，标记搜索器为已初始化")
                        searcher.initialized = True
                        return True
                    else:
                        logger.info("索引存在但无数据，需要重新加载数据")
                else:
                    logger.info("索引不存在，需要创建索引")
            except Exception as e:
                logger.warning(f"检查索引状态时出错: {e}")
        
        # 如果确实需要初始化，则进行初始化
        logger.warning("尝试重新初始化搜索器...")
        try:
            # 直接调用 create_index_with_data 来重新创建
            result = searcher.create_index_with_data(
                excel_path=None,
                force_recreate=False
            )
            
            if result.get('success', False):
                logger.info(f"✅ 搜索器重新初始化成功: {result.get('message', '')}")
                return True
            else:
                logger.error(f"❌ 搜索器重新初始化失败: {result.get('message', '未知错误')}")
                return False
        except Exception as e:
            logger.error(f"❌ 重新初始化过程中出错: {e}")
            return False
    return True


@router.get("/fields", response_model=SearchResponse, summary="搜索元数据字段")
async def search_fields(
    q: str = Query(..., description="搜索查询"),
    table_name: Optional[List[str]] = Query(None, description="限制搜索的表名列表，支持多表选择"),
    enabled_only: bool = Query(True, description="仅搜索启用字段"),
    size: int = Query(10, ge=1, le=100, description="返回结果数量"),
    use_tokenization: bool = Query(True, description="是否对查询进行分词处理"),
    tokenizer_type: str = Query("ik_max_word", description="分词器类型：ik_smart/ik_max_word/standard"),
    search_method: str = Query("hybrid", description="搜索方法：hybrid/elasticsearch/ac_matcher/similarity"),
    highlight: bool = Query(True, description="是否返回高亮信息"),
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    搜索元数据字段 - 支持混合检索和分词控制
    
    ## 搜索方法说明
    - **hybrid**: 混合搜索，结合多种搜索算法
    - **elasticsearch**: 仅使用Elasticsearch全文搜索
    - **ac_matcher**: 仅使用AC自动机精确匹配
    - **similarity**: 仅使用相似度匹配
    
    ## 分词控制说明
    - **use_tokenization=true**: 对查询进行分词处理，适合长文本和复杂查询
    - **use_tokenization=false**: 不分词，进行精确匹配，适合专业术语搜索
    - **tokenizer_type**: 分词器类型，仅在use_tokenization=true时生效
    
    ## 多表选择说明
    - **单表**: `?table_name=用户表`
    - **多表**: `?table_name=用户表&table_name=客户表&table_name=订单表`
    """
    try:
        logger.info(f"搜索请求: query='{q}', method='{search_method}', tokenization={use_tokenization}")
        
        # 从查询中移除时间部分
        cleaned_query = remove_time_from_query(q)
        
        # 确保搜索器已准备就绪
        if not ensure_searcher_ready(searcher):
            raise HTTPException(
                status_code=503, 
                detail="搜索引擎初始化失败，请检查数据文件是否存在或联系管理员"
            )
        
        # 创建搜索请求
        request = SearchRequest(
            query=cleaned_query,
            table_name=table_name,
            enabled_only=enabled_only,
            size=size,
            use_tokenization=use_tokenization,
            tokenizer_type=tokenizer_type,
            search_method=search_method,
            highlight=highlight
        )
        
        # 执行搜索
        response = searcher.search(request)
        
        logger.info(f"搜索完成: 找到 {response.total} 个结果，耗时 {response.took}ms，使用方法 {response.search_methods}")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"搜索失败: {e}")
        raise HTTPException(status_code=500, detail=f"搜索失败: {str(e)}")


@router.post("/fields", response_model=SearchResponse, summary="POST方式搜索字段")
async def search_fields_post(
    request: SearchRequest,
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    POST方式搜索字段（支持复杂查询参数）
    """
    try:
        logger.info(f"POST搜索请求: {request.model_dump()}")
        
        # 从查询中移除时间部分
        request.query = remove_time_from_query(request.query)
        
        # 确保搜索器已准备就绪
        if not ensure_searcher_ready(searcher):
            raise HTTPException(
                status_code=503, 
                detail="搜索引擎初始化失败，请检查数据文件是否存在或联系管理员"
            )
        
        response = searcher.search(request)
        
        logger.info(f"POST搜索完成: 找到 {response.total} 个结果，耗时 {response.took}ms")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"POST搜索失败: {e}")
        raise HTTPException(status_code=500, detail=f"搜索失败: {str(e)}")


@router.get("/tokenize", response_model=TokenizationResult, summary="文本分词")
async def tokenize_text(
    text: str = Query(..., description="待分词文本"),
    tokenizer_type: str = Query("ik_max_word", description="分词器类型"),
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    对文本进行分词处理
    
    ## 分词器类型
    - **ik_max_word**: IK最大词长分词器（推荐）
    - **ik_smart**: IK智能分词器
    - **standard**: 标准分词器
    """
    try:
        if not searcher.es_engine:
            raise HTTPException(status_code=503, detail="Elasticsearch引擎不可用")
        
        result = searcher.es_engine.tokenize_text(text, tokenizer_type)
        return result
        
    except Exception as e:
        logger.error(f"分词失败: {e}")
        raise HTTPException(status_code=500, detail=f"分词失败: {str(e)}")


@router.get("/suggest", summary="搜索建议")
async def get_search_suggestions(
    q: str = Query(..., description="搜索查询"),
    size: int = Query(5, ge=1, le=20, description="建议数量"),
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    获取搜索建议
    """
    try:
        # 确保搜索器已准备就绪
        if not ensure_searcher_ready(searcher):
            raise HTTPException(
                status_code=503, 
                detail="搜索引擎初始化失败，请检查数据文件是否存在或联系管理员"
            )
        
        # 使用混合搜索获取建议
        request = SearchRequest(
            query=q,
            size=size,
            search_method="hybrid",
            use_tokenization=True
        )
        
        response = searcher.search(request)
        
        suggestions = []
        for result in response.results:
            field = result.field
            suggestions.append({
                "text": field.chinese_name,
                "value": field.column_name,
                "table": field.table_name,
                "score": result.score,
                "search_method": result.search_method
            })
        
        return {
            "query": q,
            "suggestions": suggestions,
            "took": response.took,
            "search_methods": response.search_methods
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取搜索建议失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取建议失败: {str(e)}")


@router.get("/tables", summary="获取表列表")
async def get_tables(
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    获取所有表的列表
    """
    try:
        if not searcher.es_engine or not searcher.es_engine.index_exists():
            raise HTTPException(status_code=404, detail="索引不存在")
        
        # 使用Elasticsearch聚合查询获取表列表
        es = searcher.es_engine.es
        response = es.search(
            index=searcher.es_engine.index_name,
            body={
                "size": 0,
                "aggs": {
                    "tables": {
                        "terms": {
                            "field": "table_name",
                            "size": 1000
                        }
                    }
                }
            }
        )
        
        tables = []
        for bucket in response['aggregations']['tables']['buckets']:
            tables.append({
                "name": bucket['key'],
                "count": bucket['doc_count']
            })
        
        return {
            "tables": tables,
            "total": len(tables)
        }
        
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取表列表失败: {str(e)}")


@router.get("/extract", summary="提取实体")
async def extract_entities(
    text: str = Query(..., description="待提取实体的文本"),
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    从文本中提取实体
    """
    try:
        # 确保搜索器已准备就绪
        if not ensure_searcher_ready(searcher):
            raise HTTPException(
                status_code=503, 
                detail="搜索引擎初始化失败，请检查数据文件是否存在或联系管理员"
            )
        
        entities = searcher.extract_entities(text)
        
        return {
            "text": text,
            "entities": entities,
            "count": len(entities)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"实体提取失败: {e}")
        raise HTTPException(status_code=500, detail=f"实体提取失败: {str(e)}")


@router.post("/index/create", response_model=IndexResponse, summary="创建索引并加载数据")
async def create_index_with_data(
    request: IndexRequest,
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    手动创建索引并自动加载数据
    
    ## 功能说明
    - 创建Elasticsearch索引（元数据字段索引 + 指标索引）
    - 从Excel文件加载元数据和指标数据
    - 初始化所有搜索引擎（ES、AC自动机、相似度匹配器）
    - 返回详细的统计信息
    
    注意：系统会在首次访问时自动初始化，通常不需要手动调用此接口
    """
    try:
        logger.info(f"手动创建索引请求: {request.model_dump()}")
        
        # 创建元数据字段索引
        result = searcher.create_index_with_data(
            excel_path=request.excel_path,
            force_recreate=request.force_recreate
        )
        
        # 同时创建指标索引
        metric_result = None
        try:
            logger.info("同时创建指标索引...")
            metric_result = searcher.create_and_load_metrics(
                force_recreate=request.force_recreate
            )
            
            if metric_result['success']:
                logger.info(f"✅ 指标索引创建成功: {metric_result['message']}")
                # 合并统计信息
                if result.get('stats'):
                    result['stats']['metric_indexing'] = metric_result.get('stats', {})
                else:
                    result['stats'] = {'metric_indexing': metric_result.get('stats', {})}
                
                # 更新消息
                result['message'] = f"{result['message']}；{metric_result['message']}"
            else:
                logger.warning(f"指标索引创建失败: {metric_result['message']}")
                
        except Exception as me:
            logger.warning(f"指标索引创建出错（不影响主流程）: {me}")
        
        # 重置初始化标志，确保下次检查时能获得最新状态
        global _initialization_attempted
        _initialization_attempted = False
        
        return IndexResponse(
            success=result['success'],
            message=result['message'],
            stats=result.get('stats'),
            took=result['took']
        )
        
    except Exception as e:
        logger.error(f"手动创建索引失败: {e}")
        return IndexResponse(
            success=False,
            message=f"创建索引失败: {str(e)}",
            took=0
        )


@router.get("/stats", summary="获取系统统计信息")
async def get_system_stats(
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    获取系统统计信息
    """
    try:
        stats = searcher.get_stats()
        return stats
        
    except Exception as e:
        logger.error(f"获取统计信息失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取统计信息失败: {str(e)}")


@router.get("/health", summary="健康检查")
async def health_check(
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    系统健康检查
    """
    try:
        health_status = {
            "status": "healthy",
            "timestamp": "2024-01-01T00:00:00Z",
            "services": {}
        }
        
        # 检查各个服务状态
        if searcher.es_engine:
            try:
                health_status["services"]["elasticsearch"] = {
                    "status": "healthy" if searcher.es_engine.index_exists() else "index_missing",
                    "index_exists": searcher.es_engine.index_exists()
                }
            except Exception as e:
                health_status["services"]["elasticsearch"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
        
        health_status["services"]["ac_matcher"] = {
            "status": "healthy" if (searcher.ac_matcher and searcher.ac_matcher.initialized) else "not_initialized"
        }
        
        health_status["services"]["similarity"] = {
            "status": "healthy" if (searcher.similarity_matcher and searcher.similarity_matcher.initialized) else "not_initialized"
        }
        
        # 判断整体状态
        unhealthy_services = [k for k, v in health_status["services"].items() if v["status"] != "healthy"]
        if unhealthy_services:
            health_status["status"] = "degraded"
            health_status["issues"] = unhealthy_services
        
        return health_status
        
    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": "2024-01-01T00:00:00Z"
        }


@router.get("/dimension-values", response_model=SearchResponse, 
            summary="搜索维度值", description="在维度值索引中搜索特定的维度值")
async def search_dimension_values(
    q: str = Query(..., description="搜索查询"),
    table_name: Optional[List[str]] = Query(None, description="限制搜索的表名列表，支持多表选择"),
    column_name: Optional[str] = Query(None, description="限制搜索的列名"),
    size: int = Query(10, ge=1, le=100, description="返回结果数量"),
    use_tokenization: bool = Query(True, description="是否使用分词"),
    tokenizer_type: str = Query("ik_max_word", description="分词器类型"),
    highlight: bool = Query(True, description="是否返回高亮信息")
):
    """
    搜索维度值 - 在维度值索引中查找匹配的维度值
    
    ## 多表选择说明
    - **单表**: `?table_name=用户表`
    - **多表**: `?table_name=用户表&table_name=客户表&table_name=订单表`
    - **不限制表**: 不传 table_name 参数
    """
    try:
        # 从查询中移除时间部分
        cleaned_query = remove_time_from_query(q)
        
        searcher = get_hybrid_searcher()
        
        # 构建搜索请求
        request = SearchRequest(
            query=cleaned_query,
            table_name=table_name,
            size=size,
            use_tokenization=use_tokenization,
            tokenizer_type=tokenizer_type,
            search_method="dimension_values",
            highlight=highlight
        )
        
        # 执行维度值搜索
        response = searcher.search(request)
        
        return response
        
    except Exception as e:
        logger.error(f"维度值搜索失败: {e}")
        raise HTTPException(status_code=500, detail=f"维度值搜索失败: {str(e)}")


@router.post("/dimension-values", response_model=SearchResponse,
             summary="维度值POST搜索", description="使用POST方法进行维度值搜索，支持更复杂的查询参数")
async def search_dimension_values_post(request: SearchRequest):
    """维度值POST搜索 - 支持复杂的搜索参数"""
    try:
        # 从查询中移除时间部分
        request.query = remove_time_from_query(request.query)
        
        searcher = get_hybrid_searcher()
        
        # 强制设置搜索方法为维度值搜索
        request.search_method = "dimension_values"
        
        response = searcher.search(request)
        return response
        
    except Exception as e:
        logger.error(f"维度值搜索失败: {e}")
        raise HTTPException(status_code=500, detail=f"维度值搜索失败: {str(e)}")


@router.get("/database/test", 
            summary="测试数据库连接", description="测试所有配置的数据库连接是否正常")
async def test_database_connections():
    """测试数据库连接"""
    try:
        from indexing.dimension_extractor import EnhancedDimensionExtractor
        
        extractor = EnhancedDimensionExtractor()
        results = extractor.test_connections()
        extractor.close_connections()
        
        return {
            "success": True,
            "connections": results,
            "total_connections": len(results),
            "healthy_connections": len([r for r in results.values() if r.get('connected', False)])
        }
        
    except Exception as e:
        logger.error(f"数据库连接测试失败: {e}")
        raise HTTPException(status_code=500, detail=f"数据库连接测试失败: {str(e)}")

@router.post("/dimension/extract", 
             summary="手动提取维度值", description="手动触发维度值提取和索引构建")
async def extract_dimension_values(force_recreate: bool = Query(False, description="是否强制重建维度值索引")):
    """手动提取维度值并构建索引"""
    try:
        from indexing.data_loader import MetadataLoader
        from indexing.dimension_extractor import EnhancedDimensionExtractor
        
        searcher = get_hybrid_searcher()
        
        if not searcher.es_engine:
            raise HTTPException(status_code=500, detail="Elasticsearch引擎不可用")
        
        # 加载元数据
        loader = MetadataLoader()
        fields = loader.load()
        
        # 创建维度值索引
        dimension_index_created = searcher.es_engine.create_dimension_values_index(force_recreate)
        if not dimension_index_created:
            raise HTTPException(status_code=500, detail="维度值索引创建失败")
        
        # 提取维度值
        extractor = EnhancedDimensionExtractor()
        dimension_values = extractor.extract_all_dimension_values(fields)
        
        if not dimension_values:
            extractor.close_connections()
            return {
                "success": True,
                "message": "没有找到需要提取的维度值",
                "stats": {
                    "dimension_values_extracted": 0,
                    "dimension_values_indexed": 0
                }
            }
        
        # 批量索引维度值
        index_result = searcher.es_engine.bulk_index_dimension_values(dimension_values, force=force_recreate)
        extractor.close_connections()
        
        return {
            "success": True,
            "message": f"成功提取并索引了 {index_result.get('success', 0)} 个维度值",
            "stats": {
                "dimension_values_extracted": len(dimension_values),
                "dimension_values_indexed": index_result.get('success', 0),
                "dimension_index_failed": index_result.get('failed', 0)
            }
        }
        
    except Exception as e:
        logger.error(f"维度值提取失败: {e}")
        raise HTTPException(status_code=500, detail=f"维度值提取失败: {str(e)}")


# ==================== Metric（指标）相关API ====================

@router.get("/metrics",
            response_model=MetricSearchResponse,
            summary="搜索指标（GET）",
            description="通过GET方式搜索指标，支持按名称、别名、业务定义等搜索")
async def search_metrics_get(
    q: str = Query(..., description="搜索查询关键词"),
    size: int = Query(10, ge=1, le=100, description="返回结果数量"),
    use_tokenization: bool = Query(True, description="是否使用分词"),
    tokenizer_type: str = Query("ik_max_word", description="分词器类型"),
    highlight: bool = Query(True, description="是否返回高亮"),
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    搜索指标（GET方式）
    
    支持的搜索字段：
    - metric_name: 指标名称
    - metric_alias: 指标别名
    - related_entities: 相关实体
    
    示例：
    - /api/search/metrics?q=拜访次数
    - /api/search/metrics?q=销售额&size=20
    """
    try:
        # 从查询中移除时间部分
        cleaned_query = remove_time_from_query(q)
        
        # 构建搜索请求
        request = MetricSearchRequest(
            query=cleaned_query,
            size=size,
            use_tokenization=use_tokenization,
            tokenizer_type=tokenizer_type,
            highlight=highlight
        )
        
        # 执行搜索
        response = searcher.search_metrics(request)
        return response
        
    except Exception as e:
        logger.error(f"指标搜索失败: {e}")
        raise HTTPException(status_code=500, detail=f"指标搜索失败: {str(e)}")


@router.post("/metrics",
             response_model=MetricSearchResponse,
             summary="搜索指标（POST）",
             description="通过POST方式搜索指标，支持复杂查询参数")
async def search_metrics_post(
    request: MetricSearchRequest,
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    搜索指标（POST方式）
    
    支持更复杂的查询参数配置
    """
    try:
        # 从查询中移除时间部分
        request.query = remove_time_from_query(request.query)
        
        response = searcher.search_metrics(request)
        return response
        
    except Exception as e:
        logger.error(f"指标搜索失败: {e}")
        raise HTTPException(status_code=500, detail=f"指标搜索失败: {str(e)}")


# ==================== 索引管理API ====================

@router.delete("/index/delete",
               summary="删除索引",
               description="删除指定的索引或所有索引（谨慎操作）")
async def delete_indices(
    delete_fields_index: bool = Query(True, description="是否删除元数据字段索引"),
    delete_dimension_values_index: bool = Query(True, description="是否删除维度值索引"),
    delete_metrics_index: bool = Query(True, description="是否删除指标索引"),
    confirm: bool = Query(False, description="确认删除（必须设置为true才能执行删除）"),
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    删除索引
    
    **⚠️ 警告**: 这是一个危险操作，会永久删除索引数据！
    
    ## 参数说明
    - delete_fields_index: 是否删除元数据字段索引
    - delete_dimension_values_index: 是否删除维度值索引
    - delete_metrics_index: 是否删除指标索引
    - confirm: 必须设置为true才能执行删除操作
    
    ## 示例
    ```
    # 删除所有索引
    DELETE /api/search/index/delete?confirm=true
    
    # 只删除指标索引
    DELETE /api/search/index/delete?delete_fields_index=false&delete_dimension_values_index=false&delete_metrics_index=true&confirm=true
    ```
    """
    try:
        # 安全检查：必须明确确认才能删除
        if not confirm:
            raise HTTPException(
                status_code=400, 
                detail="必须设置 confirm=true 参数才能执行删除操作"
            )
        
        if not searcher.es_engine:
            raise HTTPException(status_code=500, detail="Elasticsearch引擎不可用")
        
        results = {
            "deleted_indices": [],
            "failed_indices": [],
            "skipped_indices": []
        }
        
        # 删除元数据字段索引
        if delete_fields_index:
            try:
                index_name = searcher.es_engine.fields_index_name
                if searcher.es_engine.index_exists(index_name):
                    searcher.es_engine.es.indices.delete(index=index_name)
                    results["deleted_indices"].append({
                        "name": index_name,
                        "type": "元数据字段索引"
                    })
                    logger.info(f"✅ 已删除元数据字段索引: {index_name}")
                else:
                    results["skipped_indices"].append({
                        "name": index_name,
                        "type": "元数据字段索引",
                        "reason": "索引不存在"
                    })
            except Exception as e:
                results["failed_indices"].append({
                    "name": index_name,
                    "type": "元数据字段索引",
                    "error": str(e)
                })
                logger.error(f"❌ 删除元数据字段索引失败: {e}")
        
        # 删除维度值索引
        if delete_dimension_values_index:
            try:
                index_name = searcher.es_engine.dimension_values_index_name
                if searcher.es_engine.index_exists(index_name):
                    searcher.es_engine.es.indices.delete(index=index_name)
                    results["deleted_indices"].append({
                        "name": index_name,
                        "type": "维度值索引"
                    })
                    logger.info(f"✅ 已删除维度值索引: {index_name}")
                else:
                    results["skipped_indices"].append({
                        "name": index_name,
                        "type": "维度值索引",
                        "reason": "索引不存在"
                    })
            except Exception as e:
                results["failed_indices"].append({
                    "name": index_name,
                    "type": "维度值索引",
                    "error": str(e)
                })
                logger.error(f"❌ 删除维度值索引失败: {e}")
        
        # 删除指标索引
        if delete_metrics_index:
            try:
                index_name = searcher.es_engine.metric_index_name
                if searcher.es_engine.index_exists(index_name):
                    searcher.es_engine.es.indices.delete(index=index_name)
                    results["deleted_indices"].append({
                        "name": index_name,
                        "type": "指标索引"
                    })
                    logger.info(f"✅ 已删除指标索引: {index_name}")
                else:
                    results["skipped_indices"].append({
                        "name": index_name,
                        "type": "指标索引",
                        "reason": "索引不存在"
                    })
            except Exception as e:
                results["failed_indices"].append({
                    "name": index_name,
                    "type": "指标索引",
                    "error": str(e)
                })
                logger.error(f"❌ 删除指标索引失败: {e}")
        
        # 如果删除了索引，重置搜索器的初始化状态
        if results["deleted_indices"]:
            searcher.initialized = False
            global _initialization_attempted
            _initialization_attempted = False
            logger.info("已重置搜索器初始化状态")
        
        # 构建响应消息
        success_count = len(results["deleted_indices"])
        failed_count = len(results["failed_indices"])
        skipped_count = len(results["skipped_indices"])
        
        message_parts = []
        if success_count > 0:
            message_parts.append(f"成功删除 {success_count} 个索引")
        if failed_count > 0:
            message_parts.append(f"删除失败 {failed_count} 个")
        if skipped_count > 0:
            message_parts.append(f"跳过 {skipped_count} 个（不存在）")
        
        message = "；".join(message_parts) if message_parts else "没有执行任何删除操作"
        
        return {
            "success": failed_count == 0,
            "message": message,
            "results": results,
            "summary": {
                "total_requested": sum([delete_fields_index, delete_dimension_values_index, delete_metrics_index]),
                "deleted": success_count,
                "failed": failed_count,
                "skipped": skipped_count
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除索引操作失败: {e}")
        raise HTTPException(status_code=500, detail=f"删除索引失败: {str(e)}")


# ==================== 数据同步API ====================

@router.post("/sync/metadata",
             summary="手动触发元数据同步",
             description="从API手动同步元数据到Elasticsearch")
async def sync_metadata_from_api(
    table_ids: Optional[List[int]] = Query(None, description="表ID列表，留空则使用配置的表ID"),
    jwt: Optional[str] = Query(None, description="JWT认证token，留空则使用环境变量配置的JWT"),
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    手动从API同步元数据到ES
    
    ## 参数
    - table_ids: 可选，指定要同步的表ID列表
    
    ## 注意
    - 如果不提供table_ids，将使用环境变量API_TABLE_IDS配置的表ID
    - 同步过程可能需要较长时间
    - 同步完成后会自动更新搜索引擎
    """
    try:
        from indexing.data_loader import MetadataLoader
        from core.config import config
        
        # 确定要同步的表ID
        sync_table_ids = table_ids if table_ids else []
        if not sync_table_ids:
            # 使用配置的表ID
            table_ids_str = config.API_TABLE_IDS.strip()
            if table_ids_str:
                sync_table_ids = [int(tid.strip()) for tid in table_ids_str.split(',') if tid.strip()]
        
        if not sync_table_ids:
            raise HTTPException(
                status_code=400,
                detail="未提供表ID且未配置API_TABLE_IDS环境变量"
            )
        
        logger.info(f"手动触发元数据同步，表ID: {sync_table_ids}")
        
        # 从API加载元数据
        loader = MetadataLoader(jwt=jwt)
        fields = loader.load_from_api(sync_table_ids)
        
        if not fields:
            raise HTTPException(
                status_code=500,
                detail="从API加载元数据失败或返回为空"
            )
        
        # 更新ES索引
        if not searcher.es_engine:
            raise HTTPException(status_code=500, detail="Elasticsearch引擎不可用")
        
        # 确保索引存在
        if not searcher.es_engine.index_exists():
            searcher.es_engine.create_index(force=True)
        
        # 批量索引
        index_result = searcher.es_engine.bulk_index_fields(fields)
        
        # 重新初始化搜索引擎
        if searcher.ac_matcher:
            searcher.ac_matcher.initialize(fields)
        if searcher.similarity_matcher:
            searcher.similarity_matcher.initialize(fields)
        searcher.fields_data = fields
        
        return {
            "success": True,
            "message": f"成功同步 {index_result.get('success', 0)} 个字段",
            "stats": {
                "fields_loaded": len(fields),
                "fields_indexed": index_result.get('success', 0),
                "table_ids": sync_table_ids
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"手动同步元数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"同步失败: {str(e)}")


@router.post("/sync/metrics",
             summary="手动触发指标同步",
             description="从API手动同步指标到Elasticsearch")
async def sync_metrics_from_api(
    jwt: Optional[str] = Query(None, description="JWT认证token，留空则使用环境变量配置的JWT"),
    ids: Optional[str] = Query(None, description="指标ID列表，逗号分隔（如171,172），留空则使用环境变量或加载所有"),
    force: bool = Query(True, description="是否强制重建索引（默认True）"),
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    手动从API同步指标到ES
    
    ## 注意
    - 同步过程可能需要较长时间（需要获取每个指标的详情）
    - 使用并行方式加速同步
    - ids参数示例: "171,172" 或 "171,172,173"
    """
    try:
        from indexing.data_loader import MetricLoader
        
        # 从API加载指标
        loader = MetricLoader(jwt=jwt)
        metrics = loader.load_from_api(max_workers=10, ids=ids)
        
        if not metrics:
            raise HTTPException(
                status_code=500,
                detail="从API加载指标失败或返回为空"
            )
        
        # 更新ES索引
        if not searcher.es_engine:
            raise HTTPException(status_code=500, detail="Elasticsearch引擎不可用")
        
        # 强制重建指标索引（手动同步时删除旧索引，创建新索引）
        logger.info("手动同步：删除旧指标索引并重建...")
        searcher.es_engine.create_metric_index(force=force)
        
        # 批量索引
        success = searcher.es_engine.bulk_index_metrics(metrics)
        
        if success:
            return {
                "success": True,
                "message": f"成功同步 {len(metrics)} 个指标",
                "stats": {
                    "metrics_loaded": len(metrics),
                    "metrics_indexed": len(metrics)
                }
            }
        else:
            raise HTTPException(status_code=500, detail="指标索引失败")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"手动同步指标失败: {e}")
        raise HTTPException(status_code=500, detail=f"同步失败: {str(e)}")


@router.post("/sync/dimension-values",
             summary="手动触发维度值同步",
             description="从数据库提取维度值并同步到Elasticsearch")
async def sync_dimension_values(
    jwt: Optional[str] = Query(None, description="JWT认证token，留空则使用环境变量配置的JWT"),
    force: bool = Query(True, description="是否强制重建索引（默认True）"),
    searcher: HybridSearcher = Depends(get_hybrid_searcher)
):
    """
    手动从数据库提取维度值并同步到ES
    
    ## 注意
    - 需要先确保元数据已同步（需要知道哪些字段是维度字段）
    - 会从数据库中提取所有维度字段的唯一值
    - 默认强制重建索引以确保数据干净
    """
    try:
        from indexing.data_loader import MetadataLoader
        from indexing.dimension_extractor import EnhancedDimensionExtractor
        
        # 加载元数据（获取维度字段信息）
        loader = MetadataLoader(jwt=jwt)
        fields = loader.load()
        
        if not fields:
            raise HTTPException(
                status_code=500,
                detail="加载元数据失败或返回为空"
            )
        
        # 更新ES索引
        if not searcher.es_engine:
            raise HTTPException(status_code=500, detail="Elasticsearch引擎不可用")
        
        # 强制重建维度值索引
        logger.info("手动同步：删除旧维度值索引并重建...")
        dimension_index_created = searcher.es_engine.create_dimension_values_index(force)
        
        if not dimension_index_created:
            raise HTTPException(status_code=500, detail="维度值索引创建失败")
        
        # 提取维度值
        extractor = EnhancedDimensionExtractor()
        dimension_values = extractor.extract_all_dimension_values(fields)
        extractor.close_connections()
        
        if not dimension_values:
            return {
                "success": True,
                "message": "没有找到需要提取的维度值",
                "stats": {
                    "dimension_values_extracted": 0,
                    "dimension_values_indexed": 0
                }
            }
        
        # 批量索引维度值
        index_result = searcher.es_engine.bulk_index_dimension_values(dimension_values)
        
        return {
            "success": True,
            "message": f"成功提取并索引 {index_result.get('success', 0)} 个维度值",
            "stats": {
                "dimension_values_extracted": len(dimension_values),
                "dimension_values_indexed": index_result.get('success', 0),
                "dimension_values_failed": index_result.get('failed', 0)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"手动同步维度值失败: {e}")
        raise HTTPException(status_code=500, detail=f"同步失败: {str(e)}")


@router.get("/sync/status",
            summary="获取同步状态",
            description="获取数据同步调度器的状态和最后一次同步的结果")
async def get_sync_status():
    """
    获取同步状态
    
    ## 返回信息
    - enabled: 是否启用同步
    - interval_hours: 同步间隔（小时）
    - table_ids: 配置的表ID列表
    - is_syncing: 是否正在同步
    - last_sync_time: 最后一次同步时间
    - last_sync_status: 最后一次同步的详细状态
    - scheduler_running: 调度器是否运行中
    """
    try:
        global _data_sync_scheduler
        
        if _data_sync_scheduler is None:
            from core.config import config
            return {
                "enabled": config.API_SYNC_ENABLED,
                "message": "数据同步调度器未初始化",
                "scheduler_running": False
            }
        
        status = _data_sync_scheduler.get_status()
        return status
        
    except Exception as e:
        logger.error(f"获取同步状态失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取状态失败: {str(e)}")


# ==================== 综合分析API ====================

@router.post("/comprehensive-analysis",
             response_model=ComprehensiveAnalysisResponse,
             summary="综合数据分析",
             description="对数据执行综合分析，包括基础统计、趋势、分组聚合等")
async def comprehensive_analysis_api(request: ComprehensiveAnalysisRequest):
    """
    综合数据分析接口
    
    ## 功能说明
    对提供的数据执行全方位分析，包括：
    - ✅ 基础统计（均值、中位数、标准差、偏度、峰度）
    - ✅ 四分位数分析
    - ✅ 分布分析（直方图、箱线图）
    - ✅ 趋势分析（线性回归、R²拟合度）
    - ✅ 分组聚合（按维度分组统计）
    - ✅ 分组趋势（各分组的趋势分析）
    - ❌ 异常值检测（已过滤）
    - ❌ 对比分析（已过滤）
    
    ## 请求参数
    ```json
    {
        "metric_api_address": "http://api.example.com",
        "JWT": "Bearer xxx",
        "data": {
            "rows": [{"日期": "2024-01-01", "销售额": 1000, "区域": "华东"}],
            "target_columns": ["销售额"],
            "date_column": "日期",
            "group_by": ["区域"],
            "filter_obj": {}
        }
    }
    ```
    
    ## 响应示例
    ```json
    {
        "success": true,
        "comprehensive_result": {
            "销售额": {
                "basic_stats": {...},
                "quartiles": {...},
                "trend": {...},
                "groupby_agg": [...],
                "group_trend": [...]
            }
        },
        "took": 1234
    }
    ```
    
    ## 注意事项
    - 如果 `target_columns` 为空，会自动推断数值列
    - 如果 `date_column` 存在且数据中有日期字段，会执行趋势分析
    - 如果 `group_by` 非空，会执行分组聚合和分组趋势分析
    - 分组字段和日期字段不能相同（会导致分组趋势分析失败）
    """
    import time
    from datetime import datetime
    
    start_time = time.time()
    
    try:
        # 导入 cal.py 中的 comprehensive_analysis 函数
        try:
            from indexing.cal import comprehensive_analysis
        except ImportError as e:
            logger.error(f"无法导入 comprehensive_analysis 函数: {e}")
            return ComprehensiveAnalysisResponse(
                success=False,
                error="服务器配置错误：无法加载分析模块",
                took=0
            )
        
        # 验证必填参数
        if not request.data: 
            return ComprehensiveAnalysisResponse(
                success=False,
                error="缺少必填参数: data",
                took=0
            )
        
        # 验证分组趋势配置
        group_by = request.data.get("group_by", [])
        date_column = request.data.get("date_column", "")
        
        if group_by and date_column and date_column in group_by:
            return ComprehensiveAnalysisResponse(
                success=False,
                error=f"配置错误：分组字段 {group_by} 包含日期字段 '{date_column}'。"
                      f"这会导致每个组内只有一个时间点，无法进行趋势分析。"
                      f"建议从 group_by 中移除日期字段。",
                took=0
            )
        
        # 调用综合分析函数
        logger.info(f"开始执行综合分析: target_columns={request.data.get('target_columns')}, "
                   f"date_column={date_column}, group_by={group_by}")
        
        result = comprehensive_analysis(
            metric_api_address=request.metric_api_address,
            JWT=request.JWT,
            data=request.data
        )
        
        # 检查是否有错误
        if "error" in result:
            logger.warning(f"综合分析返回错误: {result['error']}")
            return ComprehensiveAnalysisResponse(
                success=False,
                error=result["error"],
                took=int((time.time() - start_time) * 1000)
            )
        
        # 过滤结果：移除异常值检测和对比分析
        filtered_result = _filter_comprehensive_result(result)
        
        took_ms = int((time.time() - start_time) * 1000)
        logger.info(f"综合分析完成，耗时 {took_ms}ms")
        
        return ComprehensiveAnalysisResponse(
            success=True,
            comprehensive_result=filtered_result.get("comprehensive_result"),
            took=took_ms
        )
        
    except Exception as e:
        logger.error(f"综合分析失败: {e}", exc_info=True)
        took_ms = int((time.time() - start_time) * 1000)
        return ComprehensiveAnalysisResponse(
            success=False,
            error=f"分析失败: {str(e)}",
            took=took_ms
        )


def _filter_comprehensive_result(result: dict) -> dict:
    """
    过滤综合分析结果，移除异常值检测和对比分析
    保留：基础统计、四分位数、分布、趋势、分组聚合、分组趋势
    """
    if "comprehensive_result" not in result:
        return result
    
    filtered = {"comprehensive_result": {}}
    
    for column_name, column_data in result["comprehensive_result"].items():
        filtered_column = {}
        
        # 保留需要的字段
        keep_fields = ["basic_stats", "quartiles", "distribution", "trend", "groupby_agg", "group_trend"]
        
        for field in keep_fields:
            if field in column_data:
                filtered_column[field] = column_data[field]
        
        # 移除的字段：outliers, compare
        
        filtered["comprehensive_result"][column_name] = filtered_column
    
    return filtered 