"""
数据模型定义
"""

from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field


class MetadataField(BaseModel):
    """元数据字段模型"""
    table_name: str = Field(..., description="所属表名")
    column_name: str = Field(..., description="列名")
    chinese_name: str = Field(..., description="中文名称")
    alias: List[str] = Field(default_factory=list, description="别名列表")
    description: str = Field(default="", description="字段描述")
    data_type: str = Field(default="text", description="数据类型")
    field_type: str = Field(default="METRIC", description="字段类型：DIMENSION/METRIC/ATTRIBUTE")
    is_effect: bool = Field(default=True, description="是否有效")
    data_format: str = Field(default="", description="数据格式")
    sample_data: Optional[str] = Field(default=None, description="示例数据")


class DimensionValue(BaseModel):
    """维度值模型 - 用于存储维度列的具体值"""
    table_name: str = Field(..., description="所属表名")
    column_name: str = Field(..., description="列名")
    chinese_name: str = Field(..., description="维度的中文名称")
    value: str = Field(..., description="维度值")
    value_hash: Optional[str] = Field(default=None, description="值的哈希，用于去重")
    field_type: str = Field(default="dimension", description="字段类型，固定为dimension")
    data_type: str = Field(default="text", description="数据类型")
    frequency: int = Field(default=1, description="该值在源数据中的频次")
    created_at: Optional[datetime] = Field(default=None, description="创建时间")
    
    def get_search_text(self) -> str:
        """获取用于搜索的文本"""
        return f"{self.chinese_name} {self.value}"


class SearchResult(BaseModel):
    """搜索结果项"""
    field: MetadataField = Field(..., description="匹配的字段信息")
    score: float = Field(..., description="匹配分数")
    matched_text: Optional[str] = Field(default=None, description="匹配的文本片段")
    highlight: Dict[str, List[str]] = Field(default_factory=dict, description="高亮信息")
    search_method: Optional[str] = Field(default=None, description="搜索方法")
    extra_info: Dict[str, Any] = Field(default_factory=dict, description="额外信息")


class SearchResponse(BaseModel):
    """搜索响应"""
    query: str = Field(..., description="搜索查询")
    total: int = Field(..., description="总匹配数量")
    results: List[SearchResult] = Field(..., description="搜索结果列表")
    took: int = Field(..., description="搜索耗时(毫秒)")
    search_methods: Optional[List[str]] = Field(default=None, description="使用的搜索方法")
    tokenization_used: Optional[bool] = Field(default=None, description="是否使用了分词")
    tokenizer_type: Optional[str] = Field(default=None, description="使用的分词器类型")


class SearchRequest(BaseModel):
    """搜索请求"""
    query: str = Field(..., description="搜索查询")
    table_name: Optional[Union[str, List[str]]] = Field(default=None, description="限制搜索的表名，可以是单个表名或表名列表")
    enabled_only: bool = Field(default=True, description="仅搜索启用字段")
    size: int = Field(default=10, ge=1, le=100, description="返回结果数量")
    use_tokenization: bool = Field(default=True, description="是否对查询进行分词处理")
    tokenizer_type: str = Field(default="ik_max_word", description="分词器类型")
    search_method: str = Field(default="hybrid", description="搜索方法：elasticsearch/ac_matcher/similarity/hybrid")
    highlight: bool = Field(default=True, description="是否返回高亮信息")


class IndexStats(BaseModel):
    """索引统计信息"""
    total_fields: int = Field(..., description="总字段数")
    enabled_fields: int = Field(..., description="启用字段数")
    tables_count: int = Field(..., description="表数量")
    last_updated: Optional[datetime] = Field(default=None, description="最后更新时间")


class TokenizationResult(BaseModel):
    """分词结果"""
    original_text: str = Field(..., description="原始文本")
    tokens: List[str] = Field(..., description="分词结果")
    tokenizer_type: str = Field(..., description="分词器类型")
    took: int = Field(..., description="分词耗时(毫秒)")


class HybridSearchConfig(BaseModel):
    """混合搜索配置"""
    use_elasticsearch: bool = Field(default=True, description="是否使用Elasticsearch")
    use_ac_matcher: bool = Field(default=True, description="是否使用AC自动机")
    use_similarity: bool = Field(default=True, description="是否使用相似度匹配")
    weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'elasticsearch': 1.0,
            'ac_matcher': 0.9,
            'similarity': 0.8
        },
        description="各搜索方法权重"
    )
    merge_strategy: str = Field(default="weighted", description="结果合并策略：weighted/rank_fusion")


class IndexRequest(BaseModel):
    """索引请求"""
    force_recreate: bool = Field(default=False, description="是否强制重建索引")
    auto_load_data: bool = Field(default=True, description="是否自动加载数据")
    excel_path: Optional[str] = Field(default=None, description="Excel文件路径")


class IndexResponse(BaseModel):
    """索引响应"""
    success: bool = Field(..., description="是否成功")
    message: str = Field(..., description="响应消息")
    stats: Optional[Dict[str, Any]] = Field(default=None, description="索引统计")
    took: int = Field(..., description="操作耗时(毫秒)")


# ==================== Metric（指标）相关模型 ====================

class Metric(BaseModel):
    """指标数据模型"""
    metric_id: int = Field(..., description="指标ID")
    metric_name: str = Field(..., description="指标中文名")
    metric_alias: List[str] = Field(default_factory=list, description="别名列表")
    related_entities: List[str] = Field(default_factory=list, description="相关实体指标名称列表")
    metric_sql: str = Field(default="", description="完整SQL")
    depends_on_tables: List[str] = Field(default_factory=list, description="依赖的物理表列表")
    depends_on_columns: List[str] = Field(default_factory=list, description="依赖的字段列表")
    business_definition: str = Field(default="", description="业务定义/计算逻辑说明")
    
    def get_search_text(self) -> str:
        """获取用于搜索的文本"""
        alias_text = " ".join(self.metric_alias) if self.metric_alias else ""
        entities_text = " ".join(self.related_entities) if self.related_entities else ""
        return f"{self.metric_name} {alias_text} {entities_text} {self.business_definition}"


class MetricSearchResult(BaseModel):
    """Metric搜索结果项"""
    metric: Metric = Field(..., description="匹配的指标信息")
    score: float = Field(..., description="匹配分数")
    matched_text: Optional[str] = Field(default=None, description="匹配的文本片段")
    highlight: Dict[str, List[str]] = Field(default_factory=dict, description="高亮信息")
    search_method: Optional[str] = Field(default="elasticsearch", description="搜索方法")


class MetricSearchResponse(BaseModel):
    """Metric搜索响应"""
    query: str = Field(..., description="搜索查询")
    total: int = Field(..., description="总匹配数量")
    results: List[MetricSearchResult] = Field(..., description="搜索结果列表")
    took: int = Field(..., description="搜索耗时(毫秒)")
    search_methods: Optional[List[str]] = Field(default=None, description="使用的搜索方法")
    tokenization_used: Optional[bool] = Field(default=None, description="是否使用了分词")
    tokenizer_type: Optional[str] = Field(default=None, description="使用的分词器类型")


class MetricSearchRequest(BaseModel):
    """Metric搜索请求"""
    query: str = Field(..., description="搜索查询")
    size: int = Field(default=10, ge=1, le=100, description="返回结果数量")
    use_tokenization: bool = Field(default=True, description="是否对查询进行分词处理")
    tokenizer_type: str = Field(default="ik_max_word", description="分词器类型")
    highlight: bool = Field(default=True, description="是否返回高亮信息")


# ==================== 综合分析相关模型 ====================

class ComprehensiveAnalysisRequest(BaseModel):
    """综合分析请求"""
    metric_api_address: str = Field(..., description="API基础地址")
    JWT: str = Field(..., description="JWT认证token")
    data: Dict[str, Any] = Field(..., description="分析数据配置")
    
    class Config:
        json_schema_extra = {
            "example": {
                "metric_api_address": "http://localhost:8083",
                "JWT": "Bearer xxx",
                "data": {
                    "rows": [{"日期": "2024-01-01", "销售额": 1000}],
                    "target_columns": ["销售额"],
                    "date_column": "日期",
                    "group_by": ["区域"],
                    "filter_obj": {}
                }
            }
        }


class ComprehensiveAnalysisResponse(BaseModel):
    """综合分析响应"""
    success: bool = Field(..., description="是否成功")
    comprehensive_result: Optional[Dict[str, Any]] = Field(default=None, description="分析结果")
    error: Optional[str] = Field(default=None, description="错误信息")
    took: Optional[int] = Field(default=None, description="耗时（毫秒）") 