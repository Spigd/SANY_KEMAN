"""
混合搜索器 - 整合多种搜索方法
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.config import config
from core.models import (
    MetadataField, SearchResult, SearchResponse, SearchRequest,
    HybridSearchConfig, Metric, MetricSearchRequest, MetricSearchResponse
)
from .elasticsearch_engine import ElasticsearchEngine
from .ac_matcher import ACMatcher
from .similarity_matcher import SimilarityMatcher

logger = logging.getLogger(__name__)


class HybridSearcher:
    """混合搜索器 - 整合多种搜索算法"""
    
    def __init__(self, search_config: Optional[HybridSearchConfig] = None):
        """初始化混合搜索器"""
        self.config = search_config or HybridSearchConfig()
        
        # 初始化各种搜索引擎
        self.es_engine = ElasticsearchEngine() if self.config.use_elasticsearch else None
        self.ac_matcher = ACMatcher() if self.config.use_ac_matcher else None
        self.similarity_matcher = SimilarityMatcher() if self.config.use_similarity else None
        
        self.engines = {}
        if self.es_engine:
            self.engines["elasticsearch"] = self.es_engine
        if self.ac_matcher:
            self.engines["ac_matcher"] = self.ac_matcher
        if self.similarity_matcher:
            self.engines["similarity"] = self.similarity_matcher
        
        self.initialized = False
        self.fields_data = []
    
    def initialize(self, fields: List[MetadataField], force_recreate: bool = False) -> bool:
        """
        初始化所有搜索引擎
        
        Args:
            fields: 元数据字段列表
            force_recreate: 是否强制重建
        """
        try:
            logger.info("开始初始化混合搜索器...")
            self.fields_data = fields
            success_count = 0
            total_count = len(self.engines)
            
            # 初始化Elasticsearch
            if self.es_engine:
                logger.info("初始化Elasticsearch引擎...")
                if self.es_engine.create_index(force=force_recreate):
                    # 批量索引数据，传递force参数
                    index_result = self.es_engine.bulk_index_fields(fields, force=force_recreate)
                    logger.info(f"ES索引结果: {index_result}")
                    success_count += 1
                else:
                    logger.error("Elasticsearch初始化失败")
            
            # 初始化AC自动机
            if self.ac_matcher:
                logger.info("初始化AC自动机...")
                if self.ac_matcher.initialize(fields):
                    success_count += 1
                else:
                    logger.error("AC自动机初始化失败")
            
            # 初始化相似度匹配器
            if self.similarity_matcher:
                logger.info("初始化相似度匹配器...")
                if self.similarity_matcher.initialize(fields):
                    success_count += 1
                else:
                    logger.error("相似度匹配器初始化失败")
            
            self.initialized = success_count > 0
            logger.info(f"混合搜索器初始化完成: {success_count}/{total_count} 个引擎成功")
            
            return self.initialized
            
        except Exception as e:
            logger.error(f"混合搜索器初始化失败: {e}")
            self.initialized = False
            return False
    
    def search(self, request: SearchRequest) -> SearchResponse:
        """
        执行混合搜索
        
        Args:
            request: 搜索请求
        """
        if not self.initialized:
            logger.error("混合搜索器未初始化")
            return SearchResponse(
                query=request.query,
                total=0,
                results=[],
                took=0,
                search_methods=[],
                tokenization_used=request.use_tokenization,
                tokenizer_type=request.tokenizer_type if request.use_tokenization else None
            )
        
        # 根据搜索方法选择执行策略
        if request.search_method == "hybrid":
            return self._hybrid_search(request)
        elif request.search_method == "dimension_values":
            return self._dimension_values_search(request)
        elif request.search_method in self.engines:
            return self._single_engine_search(request)
        else:
            logger.error(f"不支持的搜索方法: {request.search_method}")
            return self._empty_response(request)
    
    def _hybrid_search(self, request: SearchRequest) -> SearchResponse:
        """执行混合搜索 - 并行调用多个搜索引擎"""
        start_time = datetime.now()
        
        # 并行执行多种搜索
        search_results = {}
        search_times = {}
        used_methods = []
        
        with ThreadPoolExecutor(max_workers=len(self.engines)) as executor:
            # 提交所有搜索任务
            future_to_engine = {}
            
            for engine_name, engine in self.engines.items():
                if engine_name == "elasticsearch" and self.es_engine:
                    future = executor.submit(
                        self.es_engine.search_fields,
                        query=request.query,
                        table_name=request.table_name,
                        enabled_only=request.enabled_only,
                        size=request.size * 2,  # 获取更多结果用于合并
                        use_tokenization=request.use_tokenization,
                        tokenizer_type=request.tokenizer_type,
                        highlight=request.highlight
                    )
                    future_to_engine[future] = engine_name
                
                elif engine_name == "ac_matcher" and self.ac_matcher:
                    future = executor.submit(
                        self.ac_matcher.search_fields,
                        query=request.query,
                        table_name=request.table_name,
                        enabled_only=request.enabled_only,
                        size=request.size * 2,
                        use_tokenization=request.use_tokenization
                    )
                    future_to_engine[future] = engine_name
                
                elif engine_name == "similarity" and self.similarity_matcher:
                    future = executor.submit(
                        self.similarity_matcher.search_fields,
                        query=request.query,
                        table_name=request.table_name,
                        enabled_only=request.enabled_only,
                        size=request.size * 2,
                        use_tokenization=request.use_tokenization
                    )
                    future_to_engine[future] = engine_name
            
            # 收集结果
            for future in as_completed(future_to_engine):
                engine_name = future_to_engine[future]
                try:
                    result = future.result(timeout=30)  # 30秒超时
                    search_results[engine_name] = result
                    search_times[engine_name] = result.took
                    used_methods.append(engine_name)
                    logger.info(f"{engine_name} 搜索完成: {result.total} 个结果, {result.took}ms")
                except Exception as e:
                    logger.error(f"{engine_name} 搜索失败: {e}")
                    search_results[engine_name] = self._empty_response(request)
        
        # 合并和排序结果
        merged_results = self._merge_search_results(search_results, request)
        
        # 限制最终结果数量
        final_results = merged_results[:request.size]
        
        took = int((datetime.now() - start_time).total_seconds() * 1000)
        
        return SearchResponse(
            query=request.query,
            total=len(merged_results),
            results=final_results,
            took=took,
            search_methods=used_methods,
            tokenization_used=request.use_tokenization,
            tokenizer_type=request.tokenizer_type if request.use_tokenization else None
        )
    
    def _single_engine_search(self, request: SearchRequest) -> SearchResponse:
        """执行单引擎搜索"""
        engine_name = request.search_method
        engine = self.engines.get(engine_name)
        
        if not engine:
            logger.error(f"搜索引擎 {engine_name} 不可用")
            return self._empty_response(request)
        
        try:
            if engine_name == "elasticsearch":
                return self.es_engine.search_fields(
                    query=request.query,
                    table_name=request.table_name,
                    enabled_only=request.enabled_only,
                    size=request.size,
                    use_tokenization=request.use_tokenization,
                    tokenizer_type=request.tokenizer_type,
                    highlight=request.highlight
                )
            else:
                return engine.search_fields(
                    query=request.query,
                    table_name=request.table_name,
                    enabled_only=request.enabled_only,
                    size=request.size,
                    use_tokenization=request.use_tokenization
                )
        except Exception as e:
            logger.error(f"单引擎搜索失败 {engine_name}: {e}")
            return self._empty_response(request)
    
    def _dimension_values_search(self, request: SearchRequest) -> SearchResponse:
        """执行维度值搜索"""
        if not self.es_engine:
            logger.error("Elasticsearch引擎不可用，无法执行维度值搜索")
            return self._empty_response(request)
        
        try:
            return self.es_engine.search_dimension_values(
                query=request.query,
                table_name=request.table_name,
                size=request.size,
                use_tokenization=request.use_tokenization,
                tokenizer_type=request.tokenizer_type,
                highlight=request.highlight
            )
        except Exception as e:
            logger.error(f"维度值搜索失败: {e}")
            return self._empty_response(request)
    
    def _merge_search_results(self, search_results: Dict[str, SearchResponse], 
                            request: SearchRequest) -> List[SearchResult]:
        """合并多个搜索引擎的结果"""
        all_results = []
        field_scores = {}  # 用于记录每个字段的最佳分数
        
        # 收集所有结果
        for engine_name, response in search_results.items():
            if not response or not response.results:
                continue
            
            engine_weight = self.config.weights.get(engine_name, 1.0)
            
            for result in response.results:
                field_key = f"{result.field.table_name}_{result.field.column_name}"
                
                # 应用引擎权重
                weighted_score = result.score * engine_weight
                
                # 如果是新字段或者分数更高，则更新
                if field_key not in field_scores or weighted_score > field_scores[field_key]['score']:
                    field_scores[field_key] = {
                        'result': result,
                        'score': weighted_score,
                        'original_score': result.score,
                        'engine': engine_name,
                        'weight': engine_weight
                    }
        
        # 转换为结果列表并排序
        for field_key, score_info in field_scores.items():
            result = score_info['result']
            # 更新结果的分数和搜索方法信息
            result.score = score_info['score']
            result.search_method = score_info['engine']
            
            # 添加额外信息
            result.extra_info = {
                'original_score': score_info['original_score'],
                'engine_weight': score_info['weight'],
                'weighted_score': score_info['score']
            }
            
            all_results.append(result)
        
        # 按分数排序
        all_results.sort(key=lambda x: x.score, reverse=True)
        
        return all_results
    
    def _empty_response(self, request: SearchRequest) -> SearchResponse:
        """创建空响应"""
        return SearchResponse(
            query=request.query,
            total=0,
            results=[],
            took=0,
            search_methods=[],
            tokenization_used=request.use_tokenization,
            tokenizer_type=request.tokenizer_type if request.use_tokenization else None
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """获取混合搜索器统计信息"""
        stats = {
            'initialized': self.initialized,
            'total_fields': len(self.fields_data),
            'engines': {}
        }
        
        # 获取各引擎统计
        if self.es_engine:
            try:
                es_stats = self.es_engine.get_stats()
                stats['engines']['elasticsearch'] = {
                    'available': True,
                    'stats': es_stats.model_dump()
                }
            except Exception as e:
                stats['engines']['elasticsearch'] = {
                    'available': False,
                    'error': str(e)
                }
        
        if self.ac_matcher:
            stats['engines']['ac_matcher'] = {
                'available': self.ac_matcher.initialized,
                'initialized': self.ac_matcher.initialized
            }
        
        if self.similarity_matcher:
            stats['engines']['similarity'] = {
                'available': self.similarity_matcher.initialized,
                'initialized': self.similarity_matcher.initialized
            }
        
        return stats
    
    def extract_entities(self, text: str) -> List[Dict[str, Any]]:
        """
        从文本中提取实体（使用所有可用的引擎）
        
        Args:
            text: 输入文本
        """
        all_entities = []
        
        # 从各个引擎提取实体
        if self.ac_matcher and self.ac_matcher.initialized:
            ac_entities = self.ac_matcher.extract_entities(text)
            all_entities.extend(ac_entities)
        
        if self.similarity_matcher and self.similarity_matcher.initialized:
            sim_entities = self.similarity_matcher.extract_entities(text)
            all_entities.extend(sim_entities)
        
        # 去重并按置信度排序
        unique_entities = {}
        for entity in all_entities:
            key = entity['entity'].lower()
            if key not in unique_entities or entity['confidence'] > unique_entities[key]['confidence']:
                unique_entities[key] = entity
        
        return sorted(unique_entities.values(), key=lambda x: x['confidence'], reverse=True)
    
    def create_index_with_data(self, excel_path: Optional[str] = None, 
                             force_recreate: bool = False) -> Dict[str, Any]:
        """
        创建索引并自动加载数据
        
        Args:
            excel_path: Excel文件路径
            force_recreate: 是否强制重建
        """
        start_time = datetime.now()
        
        try:
            # 如果不强制重建，先检查索引状态
            if not force_recreate:
                # 检查主字段索引是否已存在且有数据
                fields_ready = False
                dimensions_ready = False
                existing_fields_count = 0
                existing_dimensions_count = 0
                
                if self.es_engine:
                    try:
                        # 检查主字段索引
                        if self.es_engine.index_exists():
                            count_response = self.es_engine.es.count(index=self.es_engine.fields_index_name)
                            existing_fields_count = count_response.get('count', 0)
                            fields_ready = existing_fields_count > 0
                        
                        # 检查维度值索引（如果启用了维度索引功能）
                        if config.is_dimension_indexing_enabled():
                            if self.es_engine.dimension_values_index_exists():
                                dim_count_response = self.es_engine.es.count(index=self.es_engine.dimension_values_index_name)
                                existing_dimensions_count = dim_count_response.get('count', 0)
                                dimensions_ready = existing_dimensions_count > 0
                        else:
                            dimensions_ready = True  # 如果未启用维度索引，则认为已准备就绪
                        
                        # 如果所有索引都已准备就绪，直接返回
                        if fields_ready and dimensions_ready:
                            logger.info(f"所有索引已存在且有数据：字段索引 {existing_fields_count} 条，维度值索引 {existing_dimensions_count} 条，跳过重复创建")
                            self.initialized = True
                            
                            took = int((datetime.now() - start_time).total_seconds() * 1000)
                            stats = self.get_stats()
                            stats['dimension_indexing'] = {
                                'dimension_values_extracted': 0,
                                'dimension_values_indexed': existing_dimensions_count,
                                'skipped': True,
                                'message': '所有索引已存在，跳过重复创建'
                            }
                            
                            return {
                                'success': True,
                                'message': f'索引已存在：{existing_fields_count} 个字段，{existing_dimensions_count} 个维度值',
                                'stats': stats,
                                'took': took
                            }
                        else:
                            logger.info(f"索引状态检查：字段索引准备就绪={fields_ready}, 维度值索引准备就绪={dimensions_ready}")
                    except Exception as e:
                        logger.warning(f"检查索引状态时出错: {e}，将继续执行创建流程")
            
            # 导入数据加载器
            from indexing.data_loader import MetadataLoader
            
            # 加载数据
            loader = MetadataLoader(excel_path)
            fields = loader.load()
            
            if not fields:
                return {
                    'success': False,
                    'message': '未能加载数据',
                    'took': int((datetime.now() - start_time).total_seconds() * 1000)
                }
            
            # 初始化搜索器
            success = self.initialize(fields, force_recreate)
            
            # 如果启用了维度值索引且初始化成功，则提取并索引维度值
            dimension_stats = {}
            if success and config.is_dimension_indexing_enabled():
                try:
                    from indexing.dimension_extractor import EnhancedDimensionExtractor
                    
                    # 创建维度值索引
                    if self.es_engine:
                        dimension_index_created = self.es_engine.create_dimension_values_index(force_recreate)
                        if dimension_index_created:
                            logger.info("维度值索引创建成功")
                            
                            # 检查是否需要提取维度值
                            need_dimension_extraction = force_recreate
                            
                            if not force_recreate:
                                # 检查维度值索引是否已有数据
                                try:
                                    if self.es_engine.dimension_values_index_exists():
                                        count_response = self.es_engine.es.count(index=self.es_engine.dimension_values_index_name)
                                        existing_dimension_count = count_response.get('count', 0)
                                        
                                        if existing_dimension_count > 0:
                                            logger.info(f"维度值索引已存在 {existing_dimension_count} 条数据，跳过维度值提取")
                                            dimension_stats = {
                                                'dimension_values_extracted': 0,
                                                'dimension_values_indexed': existing_dimension_count,
                                                'dimension_index_failed': 0,
                                                'skipped': True,
                                                'message': f'维度值索引已存在 {existing_dimension_count} 条数据，跳过重复提取'
                                            }
                                            need_dimension_extraction = False
                                        else:
                                            logger.info("维度值索引存在但无数据，需要提取维度值")
                                            need_dimension_extraction = True
                                    else:
                                        logger.info("维度值索引不存在，需要提取维度值")
                                        need_dimension_extraction = True
                                except Exception as e:
                                    logger.warning(f"检查维度值索引状态时出错: {e}，将尝试提取维度值")
                                    need_dimension_extraction = True
                            
                            # 只有在需要时才提取维度值
                            if need_dimension_extraction:
                                logger.info("开始提取维度值...")
                                
                                # 提取维度值
                                dimension_extractor = EnhancedDimensionExtractor()
                                dimension_values = dimension_extractor.extract_all_dimension_values(fields)
                                
                                if dimension_values:
                                    # 批量索引维度值，传递force参数
                                    index_result = self.es_engine.bulk_index_dimension_values(dimension_values, force=force_recreate)
                                    dimension_stats = {
                                        'dimension_values_extracted': len(dimension_values),
                                        'dimension_values_indexed': index_result.get('success', 0),
                                        'dimension_index_failed': index_result.get('failed', 0)
                                    }
                                    logger.info(f"维度值索引完成: {dimension_stats}")
                                else:
                                    dimension_stats = {'dimension_values_extracted': 0}
                                
                                # 关闭数据库连接
                                dimension_extractor.close_connections()
                            else:
                                logger.info("✅ 维度值索引已存在，跳过提取过程")
                        else:
                            logger.warning("维度值索引创建失败")
                            dimension_stats = {'error': '维度值索引创建失败'}
                except Exception as e:
                    logger.error(f"维度值索引处理失败: {e}")
                    dimension_stats = {'error': str(e)}
            
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            
            if success:
                stats = self.get_stats()
                stats['dimension_indexing'] = dimension_stats
                
                message_parts = [f'成功创建索引并加载 {len(fields)} 个字段']
                if dimension_stats.get('dimension_values_indexed', 0) > 0:
                    message_parts.append(f'索引了 {dimension_stats["dimension_values_indexed"]} 个维度值')
                
                return {
                    'success': True,
                    'message': '，'.join(message_parts),
                    'stats': stats,
                    'took': took
                }
            else:
                return {
                    'success': False,
                    'message': '索引创建失败',
                    'took': took
                }
                
        except Exception as e:
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            logger.error(f"创建索引并加载数据失败: {e}")
            return {
                'success': False,
                'message': f'操作失败: {str(e)}',
                'took': took
            }
    
    # ==================== Metric（指标）相关方法 ====================
    
    def initialize_metrics(self, metrics: List[Metric], force_recreate: bool = False) -> bool:
        """
        初始化指标索引
        
        Args:
            metrics: 指标列表
            force_recreate: 是否强制重建索引
        
        Returns:
            是否成功
        """
        try:
            if not self.es_engine:
                logger.error("Elasticsearch引擎未初始化")
                return False
            
            # 创建Metric索引
            logger.info("开始创建指标索引...")
            index_created = self.es_engine.create_metric_index(force=force_recreate)
            
            if not index_created:
                logger.error("指标索引创建失败")
                return False
            
            # 批量索引指标数据
            if metrics:
                logger.info(f"开始索引 {len(metrics)} 个指标...")
                index_success = self.es_engine.bulk_index_metrics(metrics)
                
                if index_success:
                    logger.info(f"✅ 成功索引 {len(metrics)} 个指标")
                    return True
                else:
                    logger.error("指标数据索引失败")
                    return False
            else:
                logger.warning("没有指标数据需要索引")
                return True
                
        except Exception as e:
            logger.error(f"初始化指标失败: {e}")
            return False
    
    def search_metrics(self, request: MetricSearchRequest) -> MetricSearchResponse:
        """
        搜索指标
        
        Args:
            request: 指标搜索请求
        
        Returns:
            指标搜索响应
        """
        try:
            if not self.es_engine:
                logger.error("Elasticsearch引擎未初始化")
                return MetricSearchResponse(
                    query=request.query,
                    total=0,
                    results=[],
                    took=0,
                    search_methods=["elasticsearch"]
                )
            
            # 调用ES引擎搜索指标
            response = self.es_engine.search_metrics(
                query=request.query,
                size=request.size,
                use_tokenization=request.use_tokenization,
                tokenizer_type=request.tokenizer_type,
                highlight=request.highlight
            )
            
            return response
            
        except Exception as e:
            logger.error(f"指标搜索失败: {e}")
            return MetricSearchResponse(
                query=request.query,
                total=0,
                results=[],
                took=0,
                search_methods=["elasticsearch"]
            )
    
    def create_and_load_metrics(self, force_recreate: bool = False, 
                               excel_path: Optional[str] = None) -> Dict[str, Any]:
        """
        创建指标索引并加载数据（一键操作）
        
        Args:
            force_recreate: 是否强制重建索引
            excel_path: 指标Excel文件路径
        
        Returns:
            操作结果
        """
        start_time = datetime.now()
        
        try:
            # 加载指标数据
            from indexing.data_loader import MetricLoader
            
            loader = MetricLoader(excel_path=excel_path)
            logger.info("开始加载指标数据...")
            metrics = loader.load()
            
            if not metrics:
                return {
                    'success': False,
                    'message': '没有可加载的指标数据',
                    'took': 0
                }
            
            # 验证数据
            validation_stats = loader.validate_metrics(metrics)
            logger.info(f"指标数据验证: {validation_stats}")
            
            # 初始化指标索引
            success = self.initialize_metrics(metrics, force_recreate=force_recreate)
            
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            
            if success:
                return {
                    'success': True,
                    'message': f'成功创建指标索引并加载 {len(metrics)} 个指标',
                    'stats': validation_stats,
                    'took': took
                }
            else:
                return {
                    'success': False,
                    'message': '指标索引创建失败',
                    'took': took
                }
                
        except Exception as e:
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            logger.error(f"创建指标索引并加载数据失败: {e}")
            return {
                'success': False,
                'message': f'操作失败: {str(e)}',
                'took': took
            } 