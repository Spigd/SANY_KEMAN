"""
相似度匹配器 - 基于语义相似度的搜索
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import difflib

from core.models import MetadataField, SearchResult, SearchResponse

logger = logging.getLogger(__name__)


class SimilarityMatcher:
    """相似度匹配器"""
    
    def __init__(self):
        """初始化相似度匹配器"""
        self.fields = []
        self.search_corpus = []
        self.initialized = False
    
    def initialize(self, fields: List[MetadataField]) -> bool:
        """
        初始化相似度匹配器
        
        Args:
            fields: 元数据字段列表
        """
        try:
            self.fields = [field for field in fields if field.is_enabled]
            self.search_corpus = []
            
            # 构建搜索语料库
            for field in self.fields:
                # 收集所有可搜索的文本
                searchable_texts = []
                
                if field.chinese_name:
                    searchable_texts.append(field.chinese_name)
                
                searchable_texts.extend(field.alias)
                
                if field.description:
                    searchable_texts.append(field.description)
                
                if field.column_name:
                    searchable_texts.append(field.column_name)
                
                # 添加枚举值
                for value in field.enum_values.values():
                    if value:
                        searchable_texts.append(value)
                
                if field.sample_data:
                    searchable_texts.append(field.sample_data)
                
                # 合并所有文本作为该字段的搜索文本
                combined_text = ' '.join(searchable_texts).lower()
                self.search_corpus.append({
                    'field': field,
                    'text': combined_text,
                    'searchable_texts': searchable_texts
                })
            
            self.initialized = True
            logger.info(f"相似度匹配器初始化完成，包含 {len(self.search_corpus)} 个字段")
            return True
            
        except Exception as e:
            logger.error(f"相似度匹配器初始化失败: {e}")
            self.initialized = False
            return False
    
    def search_fields(self, query: str, table_name: Optional[str] = None,
                     entity_only: bool = False, enabled_only: bool = True,
                     size: int = 10, use_tokenization: bool = True) -> SearchResponse:
        """
        使用相似度匹配搜索字段
        
        Args:
            query: 搜索查询
            table_name: 限制搜索的表名
            entity_only: 仅搜索实体字段
            enabled_only: 仅搜索启用字段
            size: 返回结果数量
            use_tokenization: 是否使用分词（影响相似度计算）
        """
        start_time = datetime.now()
        
        if not self.initialized:
            logger.warning("相似度匹配器未初始化")
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            return SearchResponse(
                query=query,
                total=0,
                results=[],
                took=took,
                search_methods=["similarity"]
            )
        
        try:
            query_lower = query.lower()
            matches = []
            
            for corpus_item in self.search_corpus:
                field = corpus_item['field']
                field_text = corpus_item['text']
                searchable_texts = corpus_item['searchable_texts']
                
                # 应用过滤条件
                if table_name and field.table_name != table_name:
                    continue
                
                if entity_only and not field.is_entity:
                    continue
                
                if enabled_only and not field.is_enabled:
                    continue
                
                # 计算相似度分数
                similarity_score, best_match = self._calculate_similarity(
                    query_lower, field_text, searchable_texts, use_tokenization
                )
                
                if similarity_score > 0.1:  # 设置最低相似度阈值
                    matches.append({
                        'field': field,
                        'score': similarity_score,
                        'matched_text': best_match,
                        'similarity_type': 'text_similarity'
                    })
            
            # 按分数排序
            matches.sort(key=lambda x: x['score'], reverse=True)
            
            # 限制结果数量
            limited_matches = matches[:size]
            
            # 转换为SearchResult对象
            results = []
            for match in limited_matches:
                result = SearchResult(
                    field=match['field'],
                    score=match['score'],
                    matched_text=f"similarity: {match['matched_text']}",
                    highlight={},
                    search_method="similarity"
                )
                results.append(result)
            
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            
            return SearchResponse(
                query=query,
                total=len(matches),
                results=results,
                took=took,
                search_methods=["similarity"]
            )
            
        except Exception as e:
            logger.error(f"相似度匹配搜索失败: {e}")
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            return SearchResponse(
                query=query,
                total=0,
                results=[],
                took=took,
                search_methods=["similarity"]
            )
    
    def _calculate_similarity(self, query: str, field_text: str, 
                            searchable_texts: List[str], use_tokenization: bool) -> tuple:
        """
        计算查询与字段文本的相似度
        
        Args:
            query: 查询文本
            field_text: 字段组合文本
            searchable_texts: 可搜索的文本列表
            use_tokenization: 是否使用分词
        
        Returns:
            (相似度分数, 最佳匹配文本)
        """
        max_similarity = 0.0
        best_match = ""
        
        try:
            # 1. 整体文本相似度
            overall_similarity = difflib.SequenceMatcher(None, query, field_text).ratio()
            if overall_similarity > max_similarity:
                max_similarity = overall_similarity
                best_match = field_text[:50] + "..." if len(field_text) > 50 else field_text
            
            # 2. 与各个可搜索文本的相似度
            for text in searchable_texts:
                if not text:
                    continue
                
                text_lower = text.lower()
                
                # 精确匹配奖励
                if query == text_lower:
                    similarity = 1.0
                elif query in text_lower or text_lower in query:
                    similarity = 0.9
                else:
                    # 使用difflib计算相似度
                    similarity = difflib.SequenceMatcher(None, query, text_lower).ratio()
                
                # 长度相似度调整
                length_ratio = min(len(query), len(text)) / max(len(query), len(text))
                adjusted_similarity = similarity * (0.7 + 0.3 * length_ratio)
                
                if adjusted_similarity > max_similarity:
                    max_similarity = adjusted_similarity
                    best_match = text
            
            # 3. 如果使用分词，计算词级别相似度
            if use_tokenization:
                query_words = self._simple_tokenize(query)
                field_words = self._simple_tokenize(field_text)
                
                # 计算词汇重叠度
                if query_words and field_words:
                    query_set = set(query_words)
                    field_set = set(field_words)
                    
                    intersection = query_set.intersection(field_set)
                    union = query_set.union(field_set)
                    
                    if union:
                        jaccard_similarity = len(intersection) / len(union)
                        
                        # 如果词汇相似度更高，使用它
                        if jaccard_similarity > max_similarity:
                            max_similarity = jaccard_similarity
                            best_match = f"词汇匹配: {', '.join(intersection)}"
            
            return max_similarity, best_match
            
        except Exception as e:
            logger.error(f"相似度计算失败: {e}")
            return 0.0, ""
    
    def _simple_tokenize(self, text: str) -> List[str]:
        """
        简单分词（中文按字符，英文按单词）
        
        Args:
            text: 输入文本
        
        Returns:
            分词结果列表
        """
        if not text:
            return []
        
        # 移除标点符号并转换为小写
        import re
        cleaned_text = re.sub(r'[^\w\s]', ' ', text.lower())
        
        tokens = []
        words = cleaned_text.split()
        
        for word in words:
            if not word:
                continue
            
            # 如果包含中文字符，按字符分割
            if any('\u4e00' <= char <= '\u9fff' for char in word):
                tokens.extend(list(word))
            else:
                # 英文单词
                if len(word) > 1:  # 过滤单字符
                    tokens.append(word)
        
        return [token for token in tokens if len(token.strip()) > 0]
    
    def search_similar_entities(self, query: str, top_k: int = 10) -> SearchResponse:
        """
        搜索相似实体（兼容接口）
        
        Args:
            query: 查询文本
            top_k: 返回结果数量
        """
        return self.search_fields(
            query=query,
            entity_only=True,
            size=top_k
        )
    
    def extract_entities(self, text: str) -> List[Dict[str, Any]]:
        """
        从文本中提取实体（基于相似度）
        
        Args:
            text: 输入文本
        """
        if not self.initialized:
            return []
        
        entities = []
        
        try:
            # 使用相似度匹配查找实体
            response = self.search_fields(
                query=text,
                entity_only=True,
                size=10
            )
            
            for result in response.results:
                field = result.field
                
                entity = {
                    'entity': field.chinese_name,
                    'original_field': field.chinese_name,
                    'table': field.table_name,
                    'column': field.column_name,
                    'confidence': min(result.score, 1.0),  # 确保置信度不超过1
                    'match_type': 'similarity',
                    'similarity_score': result.score
                }
                entities.append(entity)
            
            return entities
            
        except Exception as e:
            logger.error(f"相似度实体提取失败: {e}")
            return [] 