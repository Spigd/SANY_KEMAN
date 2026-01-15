"""
维度值提取器 - 从源数据库提取维度列的具体值
"""

import logging
import requests
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.config import config
from core.models import MetadataField, DimensionValue
from core.database import DatabaseManager, DimensionExtractor, DatabaseConnection

logger = logging.getLogger(__name__)


class TermAPIClient:
    """术语库API客户端"""
    
    def __init__(self, api_url: Optional[str] = None):
        if api_url:
            self.api_url = api_url
        else:
            base_url = config.API_BASE_URL
            self.api_url = f"{base_url}/api/v1/terminologies/query"
            
        self.session = requests.Session()
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # 添加JWT认证头
        if config.METADATA_API_JWT:
            headers['Authorization'] = f"Bearer {config.METADATA_API_JWT}"
            
        self.session.headers.update(headers)
        
    def fetch_all_terms(self) -> Dict[str, List[str]]:
        """
        获取所有启用的术语及其描述
        
        Returns:
            Dict[str, List[str]]: {term: [description1, description2, ...]}
        """
        if not self.api_url:
            logger.warning("未配置API地址，跳过术语提取")
            return {}
            
        try:
            term_map = {}
            skip = 0
            limit = 100
            total_fetched = 0
            
            logger.info(f"开始从术语库API获取数据: {self.api_url}")
            
            while True:
                # 构造请求体，假设支持skip和limit参数
                payload = {
                    "skip": skip,
                    "limit": limit
                }
                
                try:
                    response = self.session.post(self.api_url, json=payload, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    # 验证响应格式
                    if str(data.get('code')) != '200':
                        logger.error(f"术语库API返回错误: {data.get('message')}")
                        break
                        
                    items = data.get('payload', [])
                    if not items:
                        break
                        
                    # 处理当前批次数据
                    for item in items:
                        # 过滤逻辑：必须是启用状态
                        if not item.get('enabled'):
                            continue
                            
                        term = item.get('term')
                        description = item.get('description')
                        
                        if term and description and isinstance(description, list):
                            term_map[term] = description
                    
                    # 更新分页信息
                    batch_count = len(items)
                    total_fetched += batch_count
                    skip += batch_count
                    
                    # 检查是否还有更多数据
                    metadata = data.get('metadata', {})
                    total_count = metadata.get('totalCount')
                    
                    logger.debug(f"已获取 {total_fetched} 条术语数据 (Total: {total_count})")
                    
                    if total_count is not None and skip >= total_count:
                        break
                        
                    if batch_count < limit:  # 如果返回数量小于limit，说明是最后一页
                        break
                        
                except Exception as e:
                    logger.error(f"请求术语库API失败 (skip={skip}): {e}")
                    break
            
            logger.info(f"成功获取 {len(term_map)} 个启用的术语映射")
            return term_map
            
        except Exception as e:
            logger.error(f"获取术语数据失败: {e}")
            return {}
        finally:
            self.session.close()


class EnhancedDimensionExtractor:
    """增强的维度值提取器 - 支持多数据源并行提取"""
    
    def __init__(self):
        self.db_connections = {}
        self.extractors = {}
        self._initialize_connections()
    
    def _initialize_connections(self):
        """初始化数据库连接"""
        for name, db_config in config.DATABASE_CONFIGS.items():
            try:
                logger.info(f"  - 数据库地址: {config.DATABASE_CONFIGS['default']['host']}")
                logger.info(f"  - user: {config.DATABASE_CONFIGS['default']['user']}")

                connection = DatabaseManager.create_connection(db_config)
                if connection.connect():
                    self.db_connections[name] = connection
                    self.extractors[name] = DimensionExtractor(connection)
                    logger.info(f"数据库连接 '{name}' 初始化成功")
                else:
                    logger.warning(f"数据库连接 '{name}' 初始化失败")
            except Exception as e:
                logger.error(f"初始化数据库连接 '{name}' 失败: {e}")
    
    def extract_all_dimension_values(self, metadata_fields: List[MetadataField]) -> List[DimensionValue]:
        """
        从所有配置的数据源中提取维度值
        
        Args:
            metadata_fields: 元数据字段列表
            
        Returns:
            所有维度值列表
        """
        if not config.is_dimension_indexing_enabled():
            logger.info("维度值索引功能已禁用")
            return []
        
        # 筛选维度字段
        dimension_fields = [f for f in metadata_fields if f.field_type == 'DIMENSION' and f.is_effect]
        logger.info(f"待提取维度值的字段：{dimension_fields}")
        
        if not dimension_fields:
            logger.info("没有找到需要提取值的维度字段")
            return []
        
        logger.info(f"开始提取 {len(dimension_fields)} 个维度字段的值...")
        
        all_dimension_values = []
        max_values_per_column = config.DIMENSION_VALUE_INDEXING['max_values_per_column']
        
        # 按数据源分组字段
        fields_by_source = self._group_fields_by_source(dimension_fields)
        
        all_dimension_values = []

        # 顺序处理每个数据源（通常只有一个）
        for source_name, fields in fields_by_source.items():
            if source_name not in self.extractors:
                logger.warning(f"未找到数据源 '{source_name}' 的提取器，跳过")
                continue
            
            try:
                logger.info(f"开始从数据源 '{source_name}' 提取维度值...")
                source_values = self._extract_from_source(
                    source_name,
                    fields,
                    max_values_per_column
                )
                all_dimension_values.extend(source_values)
                logger.info(f"从数据源 '{source_name}' 成功提取 {len(source_values)} 个维度值")
            except Exception as e:
                logger.error(f"从数据源 '{source_name}' 提取维度值失败: {e}")

        logger.info(f"总共提取了 {len(all_dimension_values)} 个维度值")
        
        # 3. 从术语库API获取别名并丰富维度值
        if all_dimension_values:
            self._enrich_with_term_aliases(all_dimension_values)
            
        return all_dimension_values
    
    def _enrich_with_term_aliases(self, dimension_values: List[DimensionValue]):
        """
        使用术语库API数据丰富维度值别名
        
        Args:
            dimension_values: 维度值列表 (将被原地修改)
        """
        try:
            client = TermAPIClient()
            term_map = client.fetch_all_terms()
            
            if not term_map:
                return
                
            matched_count = 0
            for dv in dimension_values:
                # 匹配逻辑：一比一相同
                if dv.value in term_map:
                    dv.alias = term_map[dv.value]
                    matched_count += 1
                    
            logger.info(f"成功为 {matched_count} 个维度值匹配到术语别名")
            
        except Exception as e:
            logger.error(f"丰富维度值别名失败: {e}")

    def _group_fields_by_source(self, dimension_fields: List[MetadataField]) -> Dict[str, List[MetadataField]]:
        """
        按数据源分组字段
        
        Args:
            dimension_fields: 维度字段列表
            
        Returns:
            按数据源分组的字段字典
        """
        # 简化实现：假设所有字段都来自默认数据源
        # 实际应用中可以根据表名或其他标识符来确定数据源
        fields_by_source = {}
        
        for field in dimension_fields:
            # 这里可以根据表名前缀、配置等来确定数据源
            # 目前简单地使用默认数据源
            source_name = self._determine_data_source(field)
            
            if source_name not in fields_by_source:
                fields_by_source[source_name] = []
            fields_by_source[source_name].append(field)
        
        return fields_by_source
    
    def _determine_data_source(self, field: MetadataField) -> str:
        """
        确定字段对应的数据源
        
        Args:
            field: 元数据字段
            
        Returns:
            数据源名称
        """
        # 简化实现：使用默认数据源
        # 实际可以根据表名前缀、配置映射等来确定
        
        # 示例：根据表名前缀确定数据源
        table_name = field.table_name.lower()
        
        # 可以配置表名前缀到数据源的映射
        source_mappings = {
            'dwd_': 'default',
            'dim_': 'default',
            'ods_': 'default'
        }
        
        for prefix, source in source_mappings.items():
            if table_name.startswith(prefix):
                return source
        
        return 'default'  # 默认数据源
    
    def _extract_from_source(self, source_name: str, fields: List[MetadataField], 
                           max_values_per_column: int) -> List[DimensionValue]:
        """
        从指定数据源提取维度值
        
        Args:
            source_name: 数据源名称
            fields: 字段列表
            max_values_per_column: 每列最大值数量
            
        Returns:
            维度值列表
        """
        extractor = self.extractors.get(source_name)
        if not extractor:
            logger.warning(f"数据源 '{source_name}' 的提取器不可用")
            return []
        
        dimension_values = []
        
        for field in fields:
            try:
                values = extractor.extract_dimension_values(
                    field.table_name,
                    field.column_name,
                    field.chinese_name,
                    max_values_per_column
                )
                dimension_values.extend(values)
                
                logger.debug(f"从 {field.table_name}.{field.column_name} 提取了 {len(values)} 个值")
                
            except Exception as e:
                logger.error(f"提取字段 {field.table_name}.{field.column_name} 的维度值失败: {e}")
                continue
        
        return dimension_values
    
    def test_connections(self) -> Dict[str, Dict[str, Any]]:
        """测试所有数据库连接"""
        results = {}
        
        for name, connection in self.db_connections.items():
            try:
                is_connected = connection.test_connection()
                results[name] = {
                    'connected': is_connected,
                    'type': connection.db_type,
                    'status': 'success' if is_connected else 'failed'
                }
            except Exception as e:
                results[name] = {
                    'connected': False,
                    'error': str(e),
                    'status': 'error'
                }
        
        return results
    
    def validate_dimension_fields(self, dimension_fields: List[MetadataField]) -> Dict[str, Any]:
        """
        验证维度字段在数据库中是否存在
        
        Args:
            dimension_fields: 维度字段列表
            
        Returns:
            验证结果
        """
        validation_results = {
            'total_fields': len(dimension_fields),
            'valid_fields': 0,
            'invalid_fields': 0,
            'details': []
        }
        
        fields_by_source = self._group_fields_by_source(dimension_fields)
        
        for source_name, fields in fields_by_source.items():
            connection = self.db_connections.get(source_name)
            if not connection:
                for field in fields:
                    validation_results['invalid_fields'] += 1
                    validation_results['details'].append({
                        'table_name': field.table_name,
                        'column_name': field.column_name,
                        'source': source_name,
                        'valid': False,
                        'error': f"数据源 '{source_name}' 不可用"
                    })
                continue
            
            for field in fields:
                try:
                    is_valid = connection.validate_table_column(field.table_name, field.column_name)
                    
                    if is_valid:
                        validation_results['valid_fields'] += 1
                    else:
                        validation_results['invalid_fields'] += 1
                    
                    validation_results['details'].append({
                        'table_name': field.table_name,
                        'column_name': field.column_name,
                        'source': source_name,
                        'valid': is_valid,
                        'error': None if is_valid else "表或列不存在"
                    })
                    
                except Exception as e:
                    validation_results['invalid_fields'] += 1
                    validation_results['details'].append({
                        'table_name': field.table_name,
                        'column_name': field.column_name,
                        'source': source_name,
                        'valid': False,
                        'error': str(e)
                    })
        
        return validation_results
    
    def close_connections(self):
        """关闭所有数据库连接"""
        for name, connection in self.db_connections.items():
            try:
                connection.disconnect()
                logger.info(f"数据库连接 '{name}' 已关闭")
            except Exception as e:
                logger.error(f"关闭数据库连接 '{name}' 失败: {e}")
        
        self.db_connections.clear()
        self.extractors.clear()
    
    def __del__(self):
        """析构函数 - 确保连接被正确关闭"""
        self.close_connections() 