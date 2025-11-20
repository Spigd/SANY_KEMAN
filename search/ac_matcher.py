"""
AC自动机匹配器 - 用于快速字符串匹配
"""

import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import ahocorasick

from core.models import MetadataField, SearchResult, SearchResponse

logger = logging.getLogger(__name__)


class ACMatcher:
    """AC自动机匹配器"""
    
    def __init__(self):
        """初始化AC自动机"""
        self.automaton = None
        self.field_mapping = {}
        self.initialized = False
    
    def initialize(self, fields: List[MetadataField]) -> bool:
        """
        初始化AC自动机
        
        Args:
            fields: 元数据字段列表
        """
        try:
            self.automaton = ahocorasick.Automaton()
            self.field_mapping = {}
            
            # 构建搜索词典
            search_terms = {}
            
            for field in fields:
                if not field.is_enabled:
                    continue
                
                field_id = f"{field.table_name}_{field.column_name}"
                
                # 添加中文名称
                if field.chinese_name:
                    search_terms[field.chinese_name.lower()] = {
                        'field': field,
                        'match_type': 'chinese_name',
                        'original_text': field.chinese_name
                    }
                
                # 添加别名
                for alias_item in field.alias:
                    if alias_item:
                        search_terms[alias_item.lower()] = {
                            'field': field,
                            'match_type': 'alias',
                            'original_text': alias_item
                        }
                
                # 添加列名（如果包含中文或有意义的英文）
                if field.column_name and len(field.column_name) > 2:
                    search_terms[field.column_name.lower()] = {
                        'field': field,
                        'match_type': 'column_name',
                        'original_text': field.column_name
                    }
                
                # 添加描述中的关键词（简单处理）
                if field.description:
                    desc_words = field.description.split()
                    for word in desc_words:
                        if len(word) > 1:
                            search_terms[word.lower()] = {
                                'field': field,
                                'match_type': 'description',
                                'original_text': word
                            }
                
                # 添加枚举值
                for key, value in field.enum_values.items():
                    if value and len(value) > 1:
                        search_terms[value.lower()] = {
                            'field': field,
                            'match_type': 'enum_value',
                            'original_text': value
                        }
            
            # 将搜索词添加到AC自动机
            for idx, (term, info) in enumerate(search_terms.items()):
                self.automaton.add_word(term, (idx, info))
            
            # 构建自动机
            self.automaton.make_automaton()
            self.initialized = True
            
            logger.info(f"AC自动机初始化完成，包含 {len(search_terms)} 个搜索词")
            return True
            
        except Exception as e:
            logger.error(f"AC自动机初始化失败: {e}")
            self.initialized = False
            return False
    
    def search_fields(self, query: str, table_name: Optional[Union[str, List[str]]] = None,
                     entity_only: bool = False, enabled_only: bool = True,
                     size: int = 10, use_tokenization: bool = True) -> SearchResponse:
        """
        使用AC自动机搜索字段
        
        Args:
            query: 搜索查询
            table_name: 限制搜索的表名
            entity_only: 仅搜索实体字段
            enabled_only: 仅搜索启用字段
            size: 返回结果数量
            use_tokenization: 是否使用分词（AC自动机忽略此参数）
        """
        start_time = datetime.now()
        
        if not self.initialized or not self.automaton:
            logger.warning("AC自动机未初始化")
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            return SearchResponse(
                query=query,
                total=0,
                results=[],
                took=took,
                search_methods=["ac_matcher"]
            )
        
        try:
            # 查找匹配项
            matches = []
            query_lower = query.lower()
            
            # 使用AC自动机查找所有匹配
            for end_index, (pattern_idx, info) in self.automaton.iter(query_lower):
                field = info['field']
                match_type = info['match_type']
                original_text = info['original_text']
                
                # 应用过滤条件
                if table_name:
                    if isinstance(table_name, list):
                        if field.table_name not in table_name:
                            continue
                    else:
                        if field.table_name != table_name:
                            continue
                
                if entity_only and not field.is_entity:
                    continue
                
                if enabled_only and not field.is_enabled:
                    continue
                
                # 计算匹配分数
                score = self._calculate_score(query, original_text, match_type)
                
                matches.append({
                    'field': field,
                    'score': score,
                    'match_type': match_type,
                    'matched_text': original_text,
                    'start_pos': end_index - len(info['original_text']) + 1,
                    'end_pos': end_index
                })
            
            # 去重（同一字段可能有多个匹配）
            unique_matches = {}
            for match in matches:
                field_key = f"{match['field'].table_name}_{match['field'].column_name}"
                if field_key not in unique_matches or match['score'] > unique_matches[field_key]['score']:
                    unique_matches[field_key] = match
            
            # 按分数排序
            sorted_matches = sorted(unique_matches.values(), key=lambda x: x['score'], reverse=True)
            
            # 限制结果数量
            limited_matches = sorted_matches[:size]
            
            # 转换为SearchResult对象
            results = []
            for match in limited_matches:
                result = SearchResult(
                    field=match['field'],
                    score=match['score'],
                    matched_text=f"{match['match_type']}: {match['matched_text']}",
                    highlight={},
                    search_method="ac_matcher"
                )
                results.append(result)
            
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            
            return SearchResponse(
                query=query,
                total=len(unique_matches),
                results=results,
                took=took,
                search_methods=["ac_matcher"]
            )
            
        except Exception as e:
            logger.error(f"AC自动机搜索失败: {e}")
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            return SearchResponse(
                query=query,
                total=0,
                results=[],
                took=took,
                search_methods=["ac_matcher"]
            )
    
    def _calculate_score(self, query: str, matched_text: str, match_type: str) -> float:
        """
        计算匹配分数
        
        Args:
            query: 原始查询
            matched_text: 匹配的文本
            match_type: 匹配类型
        """
        base_score = 1.0
        
        # 根据匹配类型调整权重
        type_weights = {
            'chinese_name': 2.0,
            'alias': 1.8,
            'column_name': 1.5,
            'enum_value': 1.3,
            'description': 1.0
        }
        
        weight = type_weights.get(match_type, 1.0)
        
        # 根据匹配程度调整分数
        query_lower = query.lower()
        matched_lower = matched_text.lower()
        
        if query_lower == matched_lower:
            # 完全匹配
            exact_bonus = 2.0
        elif matched_lower.startswith(query_lower) or matched_lower.endswith(query_lower):
            # 前缀或后缀匹配
            exact_bonus = 1.5
        elif query_lower in matched_lower:
            # 包含匹配
            exact_bonus = 1.2
        else:
            # 部分匹配
            exact_bonus = 1.0
        
        # 长度相似度奖励
        length_similarity = min(len(query), len(matched_text)) / max(len(query), len(matched_text))
        length_bonus = 1.0 + (length_similarity - 0.5) * 0.5
        
        final_score = base_score * weight * exact_bonus * length_bonus
        return round(final_score, 3)
    
    def extract_entities(self, text: str) -> List[Dict[str, Any]]:
        """
        从文本中提取实体（兼容接口）
        
        Args:
            text: 输入文本
        """
        if not self.initialized or not self.automaton:
            return []
        
        entities = []
        text_lower = text.lower()
        
        try:
            for end_index, (pattern_idx, info) in self.automaton.iter(text_lower):
                field = info['field']
                
                # 只返回实体字段
                if not field.is_entity:
                    continue
                
                entity = {
                    'entity': info['original_text'],
                    'original_field': field.chinese_name,
                    'table': field.table_name,
                    'column': field.column_name,
                    'confidence': self._calculate_score(text, info['original_text'], info['match_type']) / 10.0,
                    'match_type': f"ac_{info['match_type']}",
                    'start_pos': end_index - len(info['original_text']) + 1,
                    'end_pos': end_index
                }
                entities.append(entity)
            
            # 按置信度排序并去重
            unique_entities = {}
            for entity in entities:
                key = entity['entity'].lower()
                if key not in unique_entities or entity['confidence'] > unique_entities[key]['confidence']:
                    unique_entities[key] = entity
            
            return sorted(unique_entities.values(), key=lambda x: x['confidence'], reverse=True)
            
        except Exception as e:
            logger.error(f"AC实体提取失败: {e}")
            return [] 