import json
import re
import requests
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from urllib.parse import urljoin
from datetime import datetime


# ==================== 配置常量 ====================

ENTITY_FILTER_CONFIG = {
    "dimension": {
        "min_score": 0,
        "max_candidates": 8,
        "confidence_threshold": 0
    },
    "dimension_value": {
        "min_score": 0,
        "max_candidates": 30,
        # "confidence_threshold": 0.25
    },
    "metric": {  # 新增：指标配置
        "min_score": 0,
        "max_candidates": 10,
        "confidence_threshold": 0
    }
}

DIMENSION_RELEVANCE_WEIGHTS = {
"region_name": 1.5,
"national_region_name": 1.2,
"syb_name": 2.0
}

CONFIDENCE_PARAMS = {
    "max_score_assumption": 100,
    "min_score_threshold": 0,
    "chinese_boost_factor": 1.2,
}


# ==================== 数据模型 ====================

@dataclass
class SearchEntity:
    table_name: str
    column_name: str
    display_name: str
    score: float
    entity_type: str = ""

    @property
    def _score(self) -> float:
        return self.score


@dataclass
class Dimension(SearchEntity):
    confidence: float = 0.0
    description: Optional[str] = None
    enum_values: Dict[str, str] = None
    synonyms: List[str] = None

    def __post_init__(self):
        if self.synonyms is None:
            self.synonyms = []
        if self.enum_values is None:
            self.enum_values = {}
        self.entity_type = 'dimension'


@dataclass
class DimensionValue(SearchEntity):
    confidence: float = 0.0
    parent_dimension: Optional[str] = None
    value: Optional[str] = None
    frequency: int = 1

    def __post_init__(self):
        self.entity_type = 'dimension_value'


@dataclass
class Metric(SearchEntity):
    """新增：指标数据类"""
    confidence: float = 0.0
    metric_id: int = 0
    metric_name: str = ""  # 新增：指标名称
    metric_alias: List[str] = None
    related_entities: List[str] = None
    metric_sql: str = ""  # 新增：指标SQL
    business_definition: str = ""
    metric_type: str = ""
    status: str = "active"
    depends_on_tables: List[str] = None
    depends_on_columns: List[str] = None

    def __post_init__(self):
        if self.metric_alias is None:
            self.metric_alias = []
        if self.related_entities is None:
            self.related_entities = []
        if self.depends_on_tables is None:
            self.depends_on_tables = []
        if self.depends_on_columns is None:
            self.depends_on_columns = []
        self.entity_type = 'metric'


@dataclass
class SearchResponse:
    data: List[SearchEntity]
    best_guess: Optional[SearchEntity] = None
    temporal: List[SearchEntity] = None
    took: int = 0
    total: int = 0

    def __post_init__(self):
        if self.temporal is None:
            self.temporal = []
        if self.best_guess is None and self.data:
            self.best_guess = self.data[0]
        self.total = len(self.data)


# ==================== 核心功能函数 ====================

def _detect_chinese(text: str) -> bool:
    chinese_pattern = re.compile(r'[\u4e00-\u9fff]+')
    return bool(chinese_pattern.search(text))


def _calculate_confidence(score: float, entity_type: str, is_chinese_query: bool) -> float:
    max_score = CONFIDENCE_PARAMS["max_score_assumption"]
    min_score = CONFIDENCE_PARAMS["min_score_threshold"]

    if score <= min_score:
        base_confidence = 0.0
    elif score >= max_score:
        base_confidence = 1.0
    else:
        base_confidence = (score - min_score) / (max_score - min_score)

    if is_chinese_query:
        base_confidence *= CONFIDENCE_PARAMS["chinese_boost_factor"]
        base_confidence = min(base_confidence, 1.0)

    return round(base_confidence, 3)


def search_entities(search_api_base: str, query: str, tables_name) -> Optional[SearchResponse]:
    """
    搜索所有类型的实体：维度、维度值、指标
    """
    try:
        fields_url = urljoin(search_api_base.rstrip('/') + '/', 'api/search/fields')
        dimension_values_url = urljoin(search_api_base.rstrip('/') + '/', 'api/search/dimension-values')
        metrics_url = urljoin(search_api_base.rstrip('/') + '/', 'api/search/metrics')  # 新增：指标搜索URL

        params_dimension = {
            'q': query,
            'table_name': tables_name,
            'size': 10,
            'search_method': 'hybrid',
            'use_tokenization': True,
            'highlight': True
        }

        params_dimension_value = {
            'q': query,
            'table_name': tables_name,
            'size': 50,
            'use_tokenization': True,
            'highlight': True
        }

        # 新增：指标搜索参数
        params_metric = {
            'q': query,
            'status': 'active',  # 只搜索活跃的指标
            'size': 20,
            'use_tokenization': True,
            'highlight': True
        }

        all_entities = []

        # 搜索维度
        try:
            response = requests.get(fields_url, params=params_dimension, timeout=30)
            response.raise_for_status()
            fields_data = response.json()

            for result in fields_data.get('results', []):
                field = result.get('field', {})
                if field.get('field_type') == 'dimension':
                    entity = Dimension(
                        table_name=field.get('table_name', ''),
                        column_name=field.get('column_name', ''),
                        display_name=field.get('display_name', ''),
                        enum_values=field.get('enum_values', {}),
                        score=result.get('score', 0.0),
                        description=field.get('description', ''),
                        synonyms=field.get('synonyms', []),
                    )
                    all_entities.append(entity)

        except requests.RequestException as e:
            print(f"维度搜索失败: {e}")
            pass  # 忽略维度搜索失败，继续其他搜索

        # 搜索维度值
        try:
            response = requests.get(dimension_values_url, params=params_dimension_value, timeout=30)
            response.raise_for_status()
            dim_values_data = response.json()

            for result in dim_values_data.get('results', []):
                field = result.get('field', {})
                extra_info = result.get('extra_info', {})

                entity = DimensionValue(
                    table_name=field.get('table_name', ''),
                    column_name=field.get('column_name', ''),
                    display_name=field.get('display_name', ''),
                    score=result.get('score', 0.0),
                    parent_dimension=field.get('column_name'),
                    value=extra_info.get('dimension_value', ''),
                    frequency=extra_info.get('frequency', 1)
                )
                all_entities.append(entity)

        except requests.RequestException as e:
            print(f"维度值搜索失败: {e}")
            pass

        # 新增：搜索指标
        try:
            response = requests.get(metrics_url, params=params_metric, timeout=30)
            response.raise_for_status()
            metrics_data = response.json()

            for result in metrics_data.get('results', []):
                metric_info = result.get('metric', {})

                entity = Metric(
                    table_name='',  # 指标不属于特定表
                    column_name=metric_info.get('metric_name', ''),
                    display_name=metric_info.get('metric_name', ''),
                    score=result.get('score', 0.0),
                    metric_id=metric_info.get('metric_id', 0),
                    metric_name=metric_info.get('metric_name', ''),  # 新增：指标名称
                    metric_alias=metric_info.get('metric_alias', []),
                    related_entities=metric_info.get('related_entities', []),
                    metric_sql=metric_info.get('metric_sql', ''),  # 新增：指标SQL
                    business_definition=metric_info.get('business_definition', ''),
                    metric_type=metric_info.get('metric_type', ''),
                    status=metric_info.get('status', 'active'),
                    depends_on_tables=metric_info.get('depends_on_tables', []),
                    depends_on_columns=metric_info.get('depends_on_columns', [])
                )
                all_entities.append(entity)

        except requests.RequestException as e:
            print(f"指标搜索失败: {e}")
            pass

        if not all_entities:
            raise Exception(f"搜索失败：未找到任何匹配的实体，查询='{query}'")

        return SearchResponse(data=all_entities)

    except requests.RequestException as e:
        raise Exception(f"API调用失败: {e}")
    except Exception as e:
        raise Exception(f"搜索实体失败: {e}")


def filter_entities(search_response: SearchResponse, is_chinese_mode: bool = True) -> Dict[str, List[Dict]]:
    """
    过滤和分类实体，包含维度、维度值和指标
    """
    dimensions = []
    dimension_values = []
    metrics = []  # 新增：指标列表

    for entity in search_response.data:
        entity_type = entity.entity_type
        config = ENTITY_FILTER_CONFIG.get(entity_type, {})

        if entity.score < config.get('min_score', 0.0):
            continue

        adjusted_score = entity.score

        # 维度值的权重调整
        if entity_type == 'dimension_value' and hasattr(entity, 'parent_dimension'):
            weight = DIMENSION_RELEVANCE_WEIGHTS.get(entity.parent_dimension, 1.0)
            adjusted_score = entity.score * weight

        confidence = _calculate_confidence(adjusted_score, entity_type, is_chinese_mode)

        if confidence < config.get('confidence_threshold', 0.0):
            continue

        entity.confidence = confidence

        # 按类型分类
        if entity_type == 'dimension':
            dimensions.append(entity)
        elif entity_type == 'dimension_value':
            dimension_values.append(entity)
        elif entity_type == 'metric':  # 新增：处理指标
            metrics.append(entity)

    # 排序
    dimensions.sort(key=lambda x: x.confidence, reverse=True)
    dimension_values.sort(key=lambda x: x.confidence, reverse=True)
    metrics.sort(key=lambda x: x.confidence, reverse=True)  # 新增：指标排序

    # 截取最大数量
    dimensions = dimensions[:ENTITY_FILTER_CONFIG['dimension']['max_candidates']]
    dimension_values = dimension_values[:ENTITY_FILTER_CONFIG['dimension_value']['max_candidates']]
    metrics = metrics[:ENTITY_FILTER_CONFIG['metric']['max_candidates']]  # 新增：指标截取

    return {
        "dimensions": [asdict(d) for d in dimensions],
        "dimension_values": [asdict(dv) for dv in dimension_values],
        "metrics": [asdict(m) for m in metrics]  # 新增：返回指标
    }


def main(search_api_base: str, query: str, tables_name, JWT: str = "", required_context: str = "", language: str = "auto") -> Dict[str, Any]:
    """
    主函数：搜索并返回所有类型的实体（维度、维度值、指标）
    """
    current_time = datetime.now()
    formatted_date = current_time.strftime("%Y年%m月%d日")

    try:
        processed_query = query.strip()
        if required_context:
            processed_query = f"{required_context} {processed_query}".strip()

        if language == "auto":
            is_chinese = _detect_chinese(processed_query)
        else:
            is_chinese = language.lower() in ['zh', 'chinese', '中文']

        search_response = search_entities(search_api_base, processed_query, tables_name)

        if not search_response or not search_response.data:
            raise Exception("未找到任何匹配的实体")

        filtered_results = filter_entities(search_response, is_chinese)

        # 新增：计算总实体数包含指标
        total_entities = (
            len(filtered_results['dimensions']) + 
            len(filtered_results['dimension_values']) + 
            len(filtered_results['metrics'])
        )

        # 新增：更新搜索信息包含指标
        search_info = (
            f"搜索'{processed_query}'找到 {total_entities} 个相关实体："
            f"{len(filtered_results['dimensions'])} 个维度，"
            f"{len(filtered_results['dimension_values'])} 个维度值，"
            f"{len(filtered_results['metrics'])} 个指标"
        )

        return {
            "search_info": search_info,
            "filtered_entities": filtered_results, 
            "time": formatted_date
        }

    except Exception as e:
        error_msg = f"实体检索失败: {str(e)}"
        raise Exception(error_msg)


# ==================== 使用示例 ====================

if __name__ == "__main__":
    # 测试示例
    search_api_base = "http://localhost:8083"
    query = "大区"
    tables_name = ["dwd_ocaep_sound_detail_wide_df"]  # 或指定表名列表

    try:
        result = main(search_api_base, query, tables_name)

        print("=" * 60)
        print("搜索信息:", result["search_info"])
        print("=" * 60)

        # 打印维度
        print(f"\n维度 ({len(result['filtered_entities']['dimensions'])} 个):")
        for dim in result['filtered_entities']['dimensions']:
            print(f"  - {dim['display_name']} (置信度: {dim['confidence']}, 分数: {dim['score']:.2f})")

        # 打印维度值
        print(f"\n维度值 ({len(result['filtered_entities']['dimension_values'])} 个):")
        for dv in result['filtered_entities']['dimension_values'][:5]:  # 只显示前5个
            print(f"  - {dv['display_name']}: {dv['value']} (置信度: {dv['confidence']}, 分数: {dv['score']:.2f})")

        # 打印指标
        print(f"\n指标 ({len(result['filtered_entities']['metrics'])} 个):")
        for metric in result['filtered_entities']['metrics']:
            print(f"  - {metric['metric_name']} ({metric['metric_type']}) (置信度: {metric['confidence']}, 分数: {metric['score']:.2f})")
            if metric['metric_alias']:
                print(f"    别名: {', '.join(metric['metric_alias'][:3])}")
            if metric['related_entities']:
                print(f"    关联实体: {', '.join(metric['related_entities'][:3])}")
            if metric['depends_on_tables']:
                print(f"    依赖表: {', '.join(metric['depends_on_tables'][:3])}")
            if metric['metric_sql']:
                sql_preview = metric['metric_sql'][:80] + "..." if len(metric['metric_sql']) > 80 else metric['metric_sql']
                print(f"    SQL: {sql_preview}")

        print("\n" + "=" * 60)
        print(f"检索时间: {result['time']}")

    except Exception as e:
        print(f"错误: {e}")