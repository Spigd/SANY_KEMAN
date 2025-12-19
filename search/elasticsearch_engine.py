"""
Elasticsearch搜索引擎 - V3增强版
支持分词控制、混合检索和维度值索引
"""

import json
import logging
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError
from elasticsearch.helpers import bulk

from core.config import config
from core.models import (
    MetadataField, DimensionValue, SearchResult, SearchResponse, IndexStats, TokenizationResult,
    Metric, MetricSearchResult, MetricSearchResponse
)

logger = logging.getLogger(__name__)


class ElasticsearchEngine:
    """Elasticsearch搜索引擎 - 支持分词控制和维度值索引"""
    
    def __init__(self):
        """初始化ES客户端"""
        self.es = Elasticsearch([config.elasticsearch_url])
        self.fields_index_name = config.metadata_index_name
        self.dimension_values_index_name = config.dimension_values_index_name
        self.metric_index_name = config.metric_index_name
        
    def _check_ik_analyzer(self) -> bool:
        """检查IK分词器是否可用"""
        try:
            response = self.es.indices.analyze(
                body={
                    "analyzer": "ik_max_word",
                    "text": "测试IK分词器"
                }
            )
            return True
        except Exception as e:
            logger.warning(f"IK分词器不可用: {e}")
            return False
    
    def tokenize_text(self, text: str, tokenizer_type: str = "ik_max_word") -> TokenizationResult:
        """
        对文本进行分词
        
        Args:
            text: 待分词文本
            tokenizer_type: 分词器类型
        """
        start_time = datetime.now()
        
        try:
            # 检查分词器是否可用
            if tokenizer_type.startswith('ik_') and not self._check_ik_analyzer():
                tokenizer_type = "standard"
                logger.warning("IK分词器不可用，切换到standard分词器")
            
            response = self.es.indices.analyze(
                body={
                    "analyzer": tokenizer_type,
                    "text": text
                }
            )
            
            tokens = [token['token'] for token in response['tokens']]
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            
            return TokenizationResult(
                original_text=text,
                tokens=tokens,
                tokenizer_type=tokenizer_type,
                took=took
            )
            
        except Exception as e:
            logger.error(f"分词失败: {e}")
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            return TokenizationResult(
                original_text=text,
                tokens=[text],  # 分词失败时返回原文本
                tokenizer_type="fallback",
                took=took
            )
    
    def index_exists(self, index_name: Optional[str] = None) -> bool:
        """检查索引是否存在"""
        try:
            target_index = index_name or self.fields_index_name
            return self.es.indices.exists(index=target_index)
        except Exception as e:
            logger.error(f"检查索引存在性失败: {e}")
            return False
    
    def dimension_values_index_exists(self) -> bool:
        """检查维度值索引是否存在"""
        return self.index_exists(self.dimension_values_index_name)
    
    def create_index(self, force: bool = False) -> bool:
        """
        创建字段索引
        
        Args:
            force: 是否强制重建索引
        """
        try:
            if not force and self.index_exists():
                logger.info(f"索引 {self.fields_index_name} 已存在且未强制重建")
                return True
            
            # 检查IK分词器可用性
            ik_available = self._check_ik_analyzer()
            analyzer = config.DEFAULT_TOKENIZER if ik_available else "standard"
            search_analyzer = config.DEFAULT_SEARCH_ANALYZER if ik_available else "standard"
            
            logger.info(f"使用分词器: {analyzer} (IK可用: {ik_available})")
            
            # 索引映射配置
            mapping = {
                "mappings": {
                    "properties": {
                        "table_name": {
                            "type": "keyword",
                            "fields": {
                                "text": {
                                    "type": "text",
                                    "analyzer": analyzer,
                                    "search_analyzer": search_analyzer
                                }
                            }
                        },
                        "column_name": {
                            "type": "keyword",
                            "fields": {
                                "text": {
                                    "type": "text",
                                    "analyzer": analyzer,
                                    "search_analyzer": search_analyzer
                                }
                            }
                        },
                        "chinese_name": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer,
                            "fields": {
                                "keyword": {"type": "keyword"},
                                "exact": {
                                    "type": "text",
                                    "analyzer": "keyword"  # 精确匹配字段
                                }
                            }
                        },
                        "alias": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer,
                            "fields": {
                                "exact": {
                                    "type": "text", 
                                    "analyzer": "keyword"
                                }
                            }
                        },
                        "description": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer
                        },
                        "data_type": {"type": "keyword"},
                        "field_type": {"type": "keyword"},
                        "is_effect": {"type": "boolean"},
                        "data_format": {"type": "keyword"},
                        "sample_data": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer
                        }
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "analysis": {
                        "analyzer": {
                            "ik_max_word": {
                                "type": "ik_max_word"
                            },
                            "ik_smart": {
                                "type": "ik_smart"
                            }
                        }
                    }
                }
            }
            
            # 删除现有索引（如果强制重建）
            if force and self.es.indices.exists(index=self.fields_index_name):
                self.es.indices.delete(index=self.fields_index_name)
                logger.info(f"删除已存在的索引: {self.fields_index_name}")
            
            # 创建新索引
            self.es.indices.create(index=self.fields_index_name, body=mapping)
            logger.info(f"成功创建索引: {self.fields_index_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建索引失败: {e}")
            return False
    
    def create_dimension_values_index(self, force: bool = False) -> bool:
        """
        创建维度值索引
        
        Args:
            force: 是否强制重建索引
        """
        try:
            if not force and self.dimension_values_index_exists():
                logger.info(f"维度值索引 {self.dimension_values_index_name} 已存在且未强制重建")
                return True
            
            # 检查IK分词器可用性
            ik_available = self._check_ik_analyzer()
            analyzer = config.DEFAULT_TOKENIZER if ik_available else "standard"
            search_analyzer = config.DEFAULT_SEARCH_ANALYZER if ik_available else "standard"
            
            logger.info(f"创建维度值索引，使用分词器: {analyzer} (IK可用: {ik_available})")
            
            # 维度值索引映射配置
            mapping = {
                "mappings": {
                    "properties": {
                        "table_name": {
                            "type": "keyword",
                            "fields": {
                                "text": {
                                    "type": "text",
                                    "analyzer": analyzer,
                                    "search_analyzer": search_analyzer
                                }
                            }
                        },
                        "column_name": {
                            "type": "keyword",
                            "fields": {
                                "text": {
                                    "type": "text",
                                    "analyzer": analyzer,
                                    "search_analyzer": search_analyzer
                                }
                            }
                        },
                        "chinese_name": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer,
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "value": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer,
                            "fields": {
                                "keyword": {"type": "keyword"},
                                "exact": {
                                    "type": "text",
                                    "analyzer": "keyword"  # 精确匹配字段
                                }
                            }
                        },
                        "value_hash": {"type": "keyword"},
                        "field_type": {"type": "keyword"},
                        "data_type": {"type": "keyword"},
                        "frequency": {"type": "integer"},
                        "search_text": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer
                        },
                        "created_at": {"type": "date"}
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "analysis": {
                        "analyzer": {
                            "ik_max_word": {
                                "type": "ik_max_word"
                            },
                            "ik_smart": {
                                "type": "ik_smart"
                            }
                        }
                    }
                }
            }
            
            # 删除现有索引（如果强制重建）
            if force and self.es.indices.exists(index=self.dimension_values_index_name):
                self.es.indices.delete(index=self.dimension_values_index_name)
                logger.info(f"删除已存在的维度值索引: {self.dimension_values_index_name}")
            
            # 创建新索引
            self.es.indices.create(index=self.dimension_values_index_name, body=mapping)
            logger.info(f"成功创建维度值索引: {self.dimension_values_index_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建维度值索引失败: {e}")
            return False
    
    def bulk_index_fields(self, fields: List[MetadataField], force: bool = False) -> Dict[str, int]:
        """
        批量索引字段
        
        Args:
            fields: 元数据字段列表
            force: 是否强制重新索引，False时如果索引已有数据则跳过
        """
        success_count = 0
        failed_count = 0
        
        actions = []
        for field in fields:
            doc = field.model_dump()
            
            doc_id = f"{field.table_name}_{field.column_name}"
            
            actions.append({
                "_index": self.fields_index_name,
                "_id": doc_id,
                "_source": doc
            })
        
        try:
            from elasticsearch.helpers import bulk
            success, failed = bulk(self.es, actions, chunk_size=100, request_timeout=60)
            success_count = success
            failed_count = len(failed) if failed else 0
            
            if failed:
                logger.error(f"批量索引部分失败: 成功 {success_count}, 失败 {failed_count}")
                for error in failed[:3]:
                    logger.error(f"索引错误: {error}")
            else:
                logger.info(f"批量索引完成: 成功 {success_count}, 失败 {failed_count}")
            
        except Exception as e:
            logger.error(f"批量索引失败: {e}")
            failed_count = len(fields)
        
        return {
            "success": success_count,
            "failed": failed_count,
            "total": len(fields)
        }
    
    def bulk_index_dimension_values(self, dimension_values: List[DimensionValue], force: bool = False) -> Dict[str, int]:
        """
        批量索引维度值
        
        Args:
            dimension_values: 维度值列表
            force: 是否强制重新索引，False时如果索引已有数据则跳过
            
        Returns:
            索引结果统计
        """
        if not dimension_values:
            return {"success": 0, "failed": 0, "total": 0}
        
        success_count = 0
        failed_count = 0
        
        actions = []
        for dim_value in dimension_values:
            doc = dim_value.model_dump()
            
            # 添加搜索文本字段
            doc['search_text'] = dim_value.get_search_text()
            
            # 使用value_hash作为文档ID，确保唯一性
            doc_id = dim_value.value_hash or f"{dim_value.table_name}_{dim_value.column_name}_{hash(dim_value.value)}"
            
            actions.append({
                "_index": self.dimension_values_index_name,
                "_id": doc_id,
                "_source": doc
            })
        
        try:
            success, failed = bulk(self.es, actions, chunk_size=100, request_timeout=60)
            success_count = success
            failed_count = len(failed) if failed else 0
            
            if failed:
                logger.error(f"维度值批量索引部分失败: 成功 {success_count}, 失败 {failed_count}")
                for error in failed[:3]:
                    logger.error(f"维度值索引错误: {error}")
            else:
                logger.info(f"维度值批量索引完成: 成功 {success_count}, 失败 {failed_count}")
            
        except Exception as e:
            logger.error(f"维度值批量索引失败: {e}")
            failed_count = len(dimension_values)
        
        return {
            "success": success_count,
            "failed": failed_count,
            "total": len(dimension_values)
        }
    
    def search_fields(self, query: str, table_name: Optional[Union[str, List[str]]] = None,
                     enabled_only: bool = True,
                     size: int = 10, use_tokenization: bool = True,
                     tokenizer_type: str = "ik_max_word",
                     highlight: bool = True) -> SearchResponse:
        """
        搜索字段 - 支持分词控制
        
        Args:
            query: 搜索查询
            table_name: 限制搜索的表名
            enabled_only: 仅搜索启用字段
            size: 返回结果数量
            use_tokenization: 是否使用分词
            tokenizer_type: 分词器类型
            highlight: 是否返回高亮
        """
        start_time = datetime.now()
        
        try:
            # 确保索引存在
            if not self.index_exists():
                raise Exception("索引不存在")
            
            # 构建搜索查询
            search_body = self._build_search_query(
                query=query,
                table_name=table_name,
                enabled_only=enabled_only,
                size=size,
                use_tokenization=use_tokenization,
                tokenizer_type=tokenizer_type,
                highlight=highlight
            )
            
            # 执行搜索
            response = self.es.search(
                index=self.fields_index_name,
                body=search_body
            )
            
            # 解析结果
            results = []
            for hit in response['hits']['hits']:
                field_data = hit['_source']
                
                field = MetadataField(**field_data)
                
                # 提取高亮和匹配文本
                highlight_info = hit.get('highlight', {})
                matched_texts = []
                
                if highlight_info:
                    for field_name, highlights in highlight_info.items():
                        for hl in highlights:
                            clean_text = hl.replace('<em>', '').replace('</em>', '')
                            matched_texts.append(f"{field_name}: {clean_text}")
                
                result = SearchResult(
                    field=field,
                    score=hit['_score'],
                    matched_text='; '.join(matched_texts) if matched_texts else None,
                    highlight=highlight_info,
                    search_method="elasticsearch"
                )
                results.append(result)
            
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            
            return SearchResponse(
                query=query,
                total=response['hits']['total']['value'],
                results=results,
                took=took,
                search_methods=["elasticsearch"],
                tokenization_used=use_tokenization,
                tokenizer_type=tokenizer_type if use_tokenization else None
            )
            
        except Exception as e:
            logger.error(f"Elasticsearch搜索失败: {e}")
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            return SearchResponse(
                query=query,
                total=0,
                results=[],
                took=took,
                search_methods=["elasticsearch"],
                tokenization_used=use_tokenization,
                tokenizer_type=tokenizer_type if use_tokenization else None
            )
    
    def _build_search_query(self, query: str, table_name: Optional[Union[str, List[str]]] = None,
                           enabled_only: bool = True,
                           size: int = 10, use_tokenization: bool = True,
                           tokenizer_type: str = "ik_max_word",
                           highlight: bool = True) -> Dict[str, Any]:
        """构建搜索查询 - 支持分词控制"""
        
        # 根据分词设置构建查询
        if use_tokenization:
            # 使用分词的分层查询：精确短语 > 完整词匹配 > 模糊匹配
            must_queries = [
                {
                    "bool": {
                        "should": [
                            # 精确短语匹配 - 最高权重
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": ["chinese_name^10", "alias^8"],
                                    "type": "phrase"
                                }
                            },
                            # 完整词匹配 - 中等权重（字段所有词都在用户问题中）
                            {
                                "bool": {
                                    "should": [
                                        # chinese_name: 字段所有词都要在查询中
                                        {
                                            "match": {
                                                "chinese_name": {
                                                    "query": query,
                                                    "boost": 5,
                                                    "operator": "and"  # 要求字段所有词都在查询中
                                                }
                                            }
                                        },
                                        # alias: 字段所有词都要在查询中
                                        {
                                            "match": {
                                                "alias": {
                                                    "query": query,
                                                    "boost": 4,
                                                    "operator": "and"  # 要求字段所有词都在查询中
                                                }
                                            }
                                        }
                                    ]
                                }
                            },
                            # 模糊匹配 - 最低权重
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": ["chinese_name^2", "alias^1.5"],
                                    "type": "best_fields",
                                    "fuzziness": "AUTO"
                                }
                            }
                        ]
                    }
                }
            ]
        else:
            # 不使用分词的精确匹配查询
            must_queries = [
                {
                    "bool": {
                        "should": [
                            {"match_phrase": {"chinese_name": query}},
                            {"match_phrase": {"alias": query}},
                            {"term": {"chinese_name.keyword": query}},
                            {"terms": {"alias.exact": [query]}}
                        ],
                        "minimum_should_match": 1
                    }
                }
            ]
        
        # 过滤条件
        filter_queries = []
        
        if table_name:
            if isinstance(table_name, list):
                # 多表选择：使用terms查询
                filter_queries.append({"terms": {"table_name": table_name}})
            else:
                # 单表选择：保持原有逻辑
                filter_queries.append({"term": {"table_name": table_name}})
        
        if enabled_only:
            filter_queries.append({"term": {"is_effect": True}})
        
        # 构建完整查询
        search_body = {
            "query": {
                "bool": {
                    "must": must_queries,
                    "filter": filter_queries
                }
            },
            "size": size,
            "_source": True
        }
        
        # 添加高亮设置
        if highlight:
            search_body["highlight"] = {
                "fields": {
                    "chinese_name": {
                        "pre_tags": ["<em>"],
                        "post_tags": ["</em>"],
                        "number_of_fragments": 0  # 返回完整字段
                    },
                    "alias": {
                        "pre_tags": ["<em>"],
                        "post_tags": ["</em>"],
                        "number_of_fragments": 0  # 对于数组，只返回匹配的元素
                    }
                }
            }
        
        return search_body
    
    def get_stats(self) -> IndexStats:
        """获取索引统计信息"""
        try:
            if not self.index_exists():
                return IndexStats(
                    total_fields=0,
                    entity_fields=0,
                    enabled_fields=0,
                    tables_count=0,
                    last_updated=None
                )
            
            # 获取各种统计数据
            total_response = self.es.count(index=self.fields_index_name)
            total_fields = total_response['count']
            
            enabled_response = self.es.count(
                index=self.fields_index_name,
                body={"query": {"term": {"is_effect": True}}}
            )
            enabled_fields = enabled_response['count']
            
            tables_response = self.es.search(
                index=self.fields_index_name,
                body={
                    "size": 0,
                    "aggs": {
                        "unique_tables": {
                            "cardinality": {
                                "field": "table_name"
                            }
                        }
                    }
                }
            )
            tables_count = tables_response['aggregations']['unique_tables']['value']
            
            return IndexStats(
                total_fields=total_fields,
                enabled_fields=enabled_fields,
                tables_count=tables_count,
                last_updated=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return IndexStats(
                total_fields=0,
                enabled_fields=0,
                tables_count=0,
                last_updated=None
            )
    
    def search_dimension_values(self, query: str, table_name: Optional[Union[str, List[str]]] = None,
                              column_name: Optional[str] = None, size: int = 10,
                              use_tokenization: bool = True, tokenizer_type: str = "ik_max_word",
                              highlight: bool = True) -> SearchResponse:
        """
        搜索维度值
        
        Args:
            query: 搜索查询
            table_name: 限制搜索的表名
            column_name: 限制搜索的列名
            size: 返回结果数量
            use_tokenization: 是否使用分词
            tokenizer_type: 分词器类型
            highlight: 是否返回高亮
        """
        start_time = datetime.now()
        
        try:
            # 确保维度值索引存在
            if not self.dimension_values_index_exists():
                logger.warning("维度值索引不存在")
                return SearchResponse(
                    query=query,
                    total=0,
                    results=[],
                    took=0,
                    search_methods=["dimension_values"]
                )
            
            # 构建搜索查询
            search_body = self._build_dimension_values_search_query(
                query=query,
                table_name=table_name,
                column_name=column_name,
                size=size,
                use_tokenization=use_tokenization,
                tokenizer_type=tokenizer_type,
                highlight=highlight
            )
            
            # 执行搜索
            response = self.es.search(
                index=self.dimension_values_index_name,
                body=search_body
            )
            
            # 解析结果
            results = []
            for hit in response['hits']['hits']:
                dimension_value_data = hit['_source']
                
                # 创建一个虚拟的MetadataField对象来保持兼容性
                field = MetadataField(
                    table_name=dimension_value_data['table_name'],
                    column_name=dimension_value_data['column_name'],
                    chinese_name=dimension_value_data.get('chinese_name', dimension_value_data.get('display_name', '')),
                    field_type='DIMENSION',
                    data_type=dimension_value_data.get('data_type', 'text'),
                    description=f"维度值: {dimension_value_data['value']}"
                )
                
                # 高亮信息
                highlight_info = hit.get('highlight', {})
                
                # 匹配文本
                matched_text = f"维度值: {dimension_value_data['value']}"
                
                result = SearchResult(
                    field=field,
                    score=hit['_score'],
                    matched_text=matched_text,
                    highlight=highlight_info,
                    search_method="dimension_values",
                    extra_info={
                        'dimension_value': dimension_value_data['value'],
                        'frequency': dimension_value_data.get('frequency', 1),
                        'value_hash': dimension_value_data.get('value_hash')
                    }
                )
                results.append(result)
            
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            
            return SearchResponse(
                query=query,
                total=response['hits']['total']['value'],
                results=results,
                took=took,
                search_methods=["dimension_values"],
                tokenization_used=use_tokenization,
                tokenizer_type=tokenizer_type if use_tokenization else None
            )
            
        except Exception as e:
            logger.error(f"维度值搜索失败: {e}")
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            return SearchResponse(
                query=query,
                total=0,
                results=[],
                took=took,
                search_methods=["dimension_values"],
                tokenization_used=use_tokenization,
                tokenizer_type=tokenizer_type if use_tokenization else None
            )
    
    def _build_dimension_values_search_query(self, query: str, table_name: Optional[Union[str, List[str]]] = None,
                                           column_name: Optional[str] = None, size: int = 10,
                                           use_tokenization: bool = True, tokenizer_type: str = "ik_max_word",
                                           highlight: bool = True) -> Dict[str, Any]:
        """构建维度值搜索查询"""
        
        # 根据分词设置构建查询
        if use_tokenization:
            # 使用分词的分层查询：精确短语 > 完整词匹配 > 模糊匹配
            must_queries = [
                {
                    "bool": {
                        "should": [
                            # 第一层：精确短语匹配 - 最高权重
                            {
                                "match_phrase": {
                                    "value": {
                                        "query": query,
                                        "boost": 10
                                    }
                                }
                            },
                            # 第二层：完整词匹配（维度值所有词都在用户问题中）- 中等权重
                            {
                                "match": {
                                    "value": {
                                        "query": query,
                                        "boost": 5
                                        # 词数越多且全匹配，得分越高
                                    }
                                }
                            },
                            # 第三层：模糊匹配 - 最低权重
                            {
                                "match": {
                                    "value": {
                                        "query": query,
                                        "fuzziness": "AUTO",
                                        "boost": 2
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        else:
            # 不使用分词的精确匹配查询
            must_queries = [
                {
                    "bool": {
                        "should": [
                            {"match_phrase": {"value": query}},
                            {"term": {"value.keyword": query}},
                            {"term": {"value.exact": query}}
                        ],
                        "minimum_should_match": 1
                    }
                }
            ]
        
        # 过滤条件
        filter_queries = []
        
        # 表名过滤
        if table_name:
            if isinstance(table_name, list):
                filter_queries.append({"terms": {"table_name": table_name}})
            else:
                filter_queries.append({"term": {"table_name": table_name}})
        
        # 列名过滤
        if column_name:
            filter_queries.append({"term": {"column_name": column_name}})
        
        # 构建查询体
        search_body = {
            "size": size,
            "query": {
                "bool": {
                    "must": must_queries,
                    "filter": filter_queries
                }
            },
            "sort": [
                {"frequency": {"order": "desc"}},  # 按频次排序
                {"_score": {"order": "desc"}}
            ]
        }
        
        # 高亮设置
        if highlight:
            search_body["highlight"] = {
                "fields": {
                    "value": {"pre_tags": ["<em>"], "post_tags": ["</em>"]}
                }
            }
        
        return search_body
    
    # ==================== Metric（指标）索引和搜索方法 ====================
    
    def metric_index_exists(self) -> bool:
        """检查指标索引是否存在"""
        return self.index_exists(self.metric_index_name)
    
    def create_metric_index(self, force: bool = False) -> bool:
        """
        创建指标索引
        
        Args:
            force: 是否强制重建索引
        """
        try:
            if not force and self.metric_index_exists():
                logger.info(f"指标索引 {self.metric_index_name} 已存在且未强制重建")
                return True
            
            # 检查IK分词器可用性
            ik_available = self._check_ik_analyzer()
            analyzer = config.DEFAULT_TOKENIZER if ik_available else "standard"
            search_analyzer = config.DEFAULT_SEARCH_ANALYZER if ik_available else "standard"
            
            logger.info(f"使用分词器: {analyzer} (IK可用: {ik_available})")
            
            # 索引映射配置
            mapping = {
                "mappings": {
                    "properties": {
                        "metric_id": {
                            "type": "keyword"
                        },
                        "metric_name": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer,
                            "fields": {
                                "keyword": {"type": "keyword"},
                                "exact": {
                                    "type": "text",
                                    "analyzer": "keyword"
                                }
                            }
                        },
                        "metric_alias": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer
                        },
                        "related_entities": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer
                        },
                        "metric_sql": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer
                        },
                        "depends_on_tables": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer,
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "depends_on_columns": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer,
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "business_definition": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer
                        },
                        "search_text": {
                            "type": "text",
                            "analyzer": analyzer,
                            "search_analyzer": search_analyzer
                        }
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "index": {
                        "max_result_window": 10000
                    }
                }
            }
            
            # 如果索引存在则先删除
            if force and self.metric_index_exists():
                logger.info(f"删除已存在的指标索引: {self.metric_index_name}")
                self.es.indices.delete(index=self.metric_index_name)
            
            # 创建索引
            self.es.indices.create(index=self.metric_index_name, body=mapping)
            logger.info(f"成功创建指标索引: {self.metric_index_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建指标索引失败: {e}")
            return False
    
    def bulk_index_metrics(self, metrics: List[Metric], force: bool = False) -> bool:
        """
        批量索引指标数据
        
        Args:
            metrics: 指标列表
            force: 是否强制更新（暂未使用，为了接口一致性保留）
        """
        try:
            if not metrics:
                logger.warning("没有要索引的指标数据")
                return False
            
            # 准备批量索引数据
            actions = []
            for metric in metrics:
                # 构建搜索文本
                search_text = metric.get_search_text()
                
                # 准备文档
                doc = {
                    "_index": self.metric_index_name,
                    "_id": str(metric.metric_id),
                    "_source": {
                        "metric_id": metric.metric_id,
                        "metric_name": metric.metric_name,
                        "metric_alias": metric.metric_alias if metric.metric_alias else [],
                        "related_entities": metric.related_entities if metric.related_entities else [],
                        "metric_sql": metric.metric_sql,
                        "depends_on_tables": metric.depends_on_tables if metric.depends_on_tables else [],
                        "depends_on_columns": metric.depends_on_columns if metric.depends_on_columns else [],
                        "business_definition": metric.business_definition,
                        "search_text": search_text
                    }
                }
                actions.append(doc)
            
            # 执行批量索引
            success, failed = bulk(self.es, actions, raise_on_error=False)
            logger.info(f"批量索引指标完成: 成功 {success} 条, 失败 {len(failed)} 条")
            
            # 刷新索引
            self.es.indices.refresh(index=self.metric_index_name)
            
            return success > 0
            
        except Exception as e:
            logger.error(f"批量索引指标失败: {e}")
            return False
    
    def search_metrics(self, query: str, size: int = 10, use_tokenization: bool = True, 
                      tokenizer_type: str = "ik_max_word", highlight: bool = True) -> MetricSearchResponse:
        """
        搜索指标
        
        Args:
            query: 搜索查询
            size: 返回结果数量
            use_tokenization: 是否使用分词
            tokenizer_type: 分词器类型
            highlight: 是否返回高亮
        """
        start_time = datetime.now()
        
        try:
            if not self.metric_index_exists():
                logger.warning(f"指标索引不存在: {self.metric_index_name}")
                return MetricSearchResponse(
                    query=query,
                    total=0,
                    results=[],
                    took=0,
                    search_methods=["elasticsearch"],
                    tokenization_used=use_tokenization,
                    tokenizer_type=tokenizer_type if use_tokenization else None
                )
            
            # 构建搜索查询
            search_body = self._build_metric_search_query(
                query=query,
                size=size,
                use_tokenization=use_tokenization,
                tokenizer_type=tokenizer_type,
                highlight=highlight
            )
            
            # 执行搜索
            response = self.es.search(index=self.metric_index_name, body=search_body)
            
            # 解析结果
            results = []
            for hit in response['hits']['hits']:
                source = hit['_source']
                
                # 构建Metric对象（添加类型检查以向后兼容旧索引）
                metric_alias_value = source.get('metric_alias', [])
                if isinstance(metric_alias_value, list):
                    metric_alias = metric_alias_value
                else:
                    metric_alias = metric_alias_value.split() if metric_alias_value else []
                
                related_entities_value = source.get('related_entities', [])
                if isinstance(related_entities_value, list):
                    related_entities = related_entities_value
                else:
                    related_entities = related_entities_value.split() if related_entities_value else []
                
                depends_on_tables_value = source.get('depends_on_tables', [])
                if isinstance(depends_on_tables_value, list):
                    depends_on_tables = depends_on_tables_value
                else:
                    depends_on_tables = depends_on_tables_value.split() if depends_on_tables_value else []
                
                depends_on_columns_value = source.get('depends_on_columns', [])
                if isinstance(depends_on_columns_value, list):
                    depends_on_columns = depends_on_columns_value
                else:
                    depends_on_columns = depends_on_columns_value.split() if depends_on_columns_value else []
                
                metric = Metric(
                    metric_id=int(source['metric_id']),
                    metric_name=source['metric_name'],
                    metric_alias=metric_alias,
                    related_entities=related_entities,
                    metric_sql=source.get('metric_sql', ''),
                    depends_on_tables=depends_on_tables,
                    depends_on_columns=depends_on_columns,
                    business_definition=source.get('business_definition', '')
                )
                
                # 处理高亮
                highlight_dict = {}
                if 'highlight' in hit:
                    highlight_dict = hit['highlight']
                
                # 确定匹配的文本
                matched_text = None
                if highlight_dict:
                    for field, highlights in highlight_dict.items():
                        if highlights:
                            matched_text = f"{field}: {highlights[0]}"
                            break
                
                if not matched_text:
                    matched_text = f"metric_name: {metric.metric_name}"
                
                result = MetricSearchResult(
                    metric=metric,
                    score=hit['_score'],
                    matched_text=matched_text,
                    highlight=highlight_dict,
                    search_method="elasticsearch"
                )
                results.append(result)
            
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            
            return MetricSearchResponse(
                query=query,
                total=response['hits']['total']['value'],
                results=results,
                took=took,
                search_methods=["elasticsearch"],
                tokenization_used=use_tokenization,
                tokenizer_type=tokenizer_type if use_tokenization else None
            )
            
        except Exception as e:
            logger.error(f"指标搜索失败: {e}")
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            return MetricSearchResponse(
                query=query,
                total=0,
                results=[],
                took=took,
                search_methods=["elasticsearch"],
                tokenization_used=use_tokenization,
                tokenizer_type=tokenizer_type if use_tokenization else None
            )
    
    def _build_metric_search_query(self, query: str, size: int = 10,
                                  use_tokenization: bool = True, tokenizer_type: str = "ik_max_word",
                                  highlight: bool = True) -> Dict[str, Any]:
        """构建指标搜索查询"""
        
        # 根据分词设置构建查询
        if use_tokenization:
            # 使用分词的分层查询：精确短语 > 完整词匹配 > 模糊匹配
            must_queries = [
                {
                    "bool": {
                        "should": [
                            # 第一层：精确短语匹配 - 最高权重
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": ["metric_name^10", "metric_alias^8", "related_entities^5"],
                                    "type": "phrase"
                                }
                            },
                            # 第二层：完整词匹配（指标所有词都在用户问题中）- 中等权重
                            {
                                "bool": {
                                    "should": [
                                        # metric_name: 指标名所有词都要在查询中
                                        {
                                            "match": {
                                                "metric_name": {
                                                    "query": query,
                                                    "boost": 5,
                                                    "operator": "and"  # 要求字段所有词都在查询中
                                                }
                                            }
                                        },
                                        # metric_alias: 指标别名所有词都要在查询中
                                        {
                                            "match": {
                                                "metric_alias": {
                                                    "query": query,
                                                    "boost": 4,
                                                    "operator": "and"  # 要求字段所有词都在查询中
                                                }
                                            }
                                        },
                                        # related_entities: 相关实体所有词都要在查询中
                                        {
                                            "match": {
                                                "related_entities": {
                                                    "query": query,
                                                    "boost": 3,
                                                    "operator": "and"  # 要求字段所有词都在查询中
                                                }
                                            }
                                        }
                                    ]
                                }
                            },
                            # 第三层：模糊匹配 - 最低权重
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": ["metric_name^2", "metric_alias^1.5", "related_entities^1"],
                                    "type": "best_fields",
                                    "fuzziness": "AUTO"
                                }
                            }
                        ]
                    }
                }
            ]
        else:
            # 不使用分词的精确匹配查询
            must_queries = [
                {
                    "bool": {
                        "should": [
                            {"match_phrase": {"metric_name": query}},
                            {"term": {"metric_name.keyword": query}},
                            {"term": {"metric_name.exact": query}},
                            {"match_phrase": {"metric_alias": query}},
                            {"match_phrase": {"related_entities": query}}
                        ],
                        "minimum_should_match": 1
                    }
                }
            ]
        
        # 过滤条件
        filter_queries = []
        
        # 构建查询体
        search_body = {
            "size": size,
            "query": {
                "bool": {
                    "must": must_queries,
                    "filter": filter_queries
                }
            },
            "sort": [
                {"_score": {"order": "desc"}}
            ]
        }
        
        # 高亮设置
        if highlight:
            search_body["highlight"] = {
                "fields": {
                    "metric_name": {
                        "pre_tags": ["<em>"],
                        "post_tags": ["</em>"],
                        "number_of_fragments": 0  # 返回完整字段
                    },
                    "metric_alias": {
                        "pre_tags": ["<em>"],
                        "post_tags": ["</em>"],
                        "number_of_fragments": 0  # 对于数组，只返回匹配的元素
                    },
                    "related_entities": {
                        "pre_tags": ["<em>"],
                        "post_tags": ["</em>"],
                        "number_of_fragments": 0
                    }
                }
            }
        
        return search_body 