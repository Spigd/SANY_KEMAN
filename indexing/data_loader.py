"""
元数据表数据加载器 - V3增强版
包含MetadataLoader和MetricLoader
"""

import json
import logging
import pandas as pd
import requests
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.config import config
from core.models import MetadataField, Metric

logger = logging.getLogger(__name__)


# ==================== API客户端类 ====================

class MetadataAPIClient:
    """元数据API客户端"""
    
    def __init__(self, base_url: Optional[str] = None, timeout: int = 30, jwt: Optional[str] = None):
        """初始化API客户端"""
        self.base_url = base_url or config.METADATA_API_BASE_URL
        self.timeout = timeout
        self.jwt = jwt or config.METADATA_API_JWT
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        })
        
        # 添加JWT认证头
        if self.jwt:
            self.session.headers.update({
                'Authorization': f'Bearer {self.jwt}'
            })
    
    def get_table_info(self, table_id: int) -> Optional[Dict[str, Any]]:
        """
        获取表信息
        
        Args:
            table_id: 表ID
            
        Returns:
            表信息字典，包含tableName等字段
        """
        try:
            url = f"{self.base_url}/api/data-tables/{table_id}"
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            if data.get('code') == '200' and data.get('payload'):
                return data['payload']
            else:
                logger.error(f"获取表信息失败: {data.get('message', '未知错误')}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"请求表信息API失败 (table_id={table_id}): {e}")
            return None
        except Exception as e:
            logger.error(f"解析表信息失败 (table_id={table_id}): {e}")
            return None
    
    def get_table_fields(self, table_id: int) -> List[Dict[str, Any]]:
        """
        获取表字段列表
        
        Args:
            table_id: 表ID
            
        Returns:
            字段信息列表
        """
        try:
            url = f"{self.base_url}/api/table-fields/table/{table_id}"
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            if data.get('code') == '200' and data.get('payload'):
                return data['payload'] if isinstance(data['payload'], list) else []
            else:
                logger.error(f"获取表字段失败: {data.get('message', '未知错误')}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"请求表字段API失败 (table_id={table_id}): {e}")
            return []
        except Exception as e:
            logger.error(f"解析表字段失败 (table_id={table_id}): {e}")
            return []
    
    def close(self):
        """关闭会话"""
        self.session.close()


class MetricAPIClient:
    """指标API客户端"""
    
    def __init__(self, base_url: Optional[str] = None, timeout: int = 30, jwt: Optional[str] = None):
        """初始化API客户端"""
        self.base_url = base_url or config.METADATA_API_BASE_URL
        self.timeout = timeout
        self.jwt = jwt or config.METADATA_API_JWT
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        })
        
        # 添加JWT认证头
        if self.jwt:
            self.session.headers.update({
                'Authorization': f'Bearer {self.jwt}'
            })
    
    def get_metrics_list(self, ids: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取指标列表
        
        Args:
            ids: 指标ID列表（逗号分隔的字符串，如 "171,172"）
            
        Returns:
            指标信息列表
        """
        try:
            url = f"{self.base_url}/api/v1/metrics"
            params = {}
            if ids:
                params['ids'] = ids
            
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            if data.get('code') == '200' and data.get('payload'):
                return data['payload'] if isinstance(data['payload'], list) else []
            else:
                logger.error(f"获取指标列表失败: {data.get('message', '未知错误')}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"请求指标列表API失败: {e}")
            return []
        except Exception as e:
            logger.error(f"解析指标列表失败: {e}")
            return []
    
    def get_metric_detail(self, metric_id: int) -> Optional[Dict[str, Any]]:
        """
        获取指标详情
        
        Args:
            metric_id: 指标ID
            
        Returns:
            指标详情字典
        """
        try:
            url = f"{self.base_url}/api/v1/metrics/{metric_id}"
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            if data.get('code') == '200' and data.get('payload'):
                return data['payload']
            else:
                logger.error(f"获取指标详情失败 (metric_id={metric_id}): {data.get('message', '未知错误')}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"请求指标详情API失败 (metric_id={metric_id}): {e}")
            return None
        except Exception as e:
            logger.error(f"解析指标详情失败 (metric_id={metric_id}): {e}")
            return None
    
    def close(self):
        """关闭会话"""
        self.session.close()


class MetadataLoader:
    """元数据表加载器"""
    
    def __init__(self, excel_path: Optional[str] = None, api_base_url: Optional[str] = None, jwt: Optional[str] = None):
        """初始化加载器"""
        self.excel_path = excel_path or config.metadata_excel_full_path
        self.api_base_url = api_base_url or config.METADATA_API_BASE_URL
        self.jwt = jwt or config.METADATA_API_JWT
        self.api_client = None
    
    def _parse_table_ids(self) -> List[int]:
        """从配置中解析表ID列表"""
        table_ids_str = config.API_TABLE_IDS.strip()
        if not table_ids_str:
            return []
        
        try:
            table_ids = [int(tid.strip()) for tid in table_ids_str.split(',') if tid.strip()]
            return table_ids
        except ValueError as e:
            logger.error(f"解析表ID列表失败: {e}")
            return []
    
    def load(self, table_ids: Optional[List[int]] = None) -> List[MetadataField]:
        """
        智能加载元数据 - 根据配置自动选择数据源
        
        Args:
            table_ids: 表ID列表（仅API模式需要），如果不提供则使用配置的表ID
            
        Returns:
            元数据字段列表
        """
        if config.API_SYNC_ENABLED:
            # 从API加载
            if not table_ids:
                # 使用配置的表ID
                table_ids = self._parse_table_ids()
            
            if table_ids:
                logger.info(f"从API加载元数据（启用API同步模式）")
                return self.load_from_api(table_ids)
            else:
                logger.warning("API同步已启用但未配置表ID，回退到Excel")
                return self.load_from_excel()
        else:
            # 从Excel加载
            logger.info("从Excel加载元数据（未启用API同步）")
            return self.load_from_excel()
    
    def load_from_api(self, table_ids: List[int]) -> List[MetadataField]:
        """
        从API加载元数据
        
        Args:
            table_ids: 表ID列表
            
        Returns:
            元数据字段列表
        """
        try:
            self.api_client = MetadataAPIClient(self.api_base_url, jwt=self.jwt)
            all_fields = []
            
            logger.info(f"开始从API加载元数据，表ID列表: {table_ids}")
            
            # 获取所有表的信息和字段
            for table_id in table_ids:
                try:
                    # 获取表信息
                    table_info = self.api_client.get_table_info(table_id)
                    if not table_info:
                        logger.warning(f"跳过表ID {table_id}，无法获取表信息")
                        continue
                    
                    table_name = table_info.get('tableName', '')
                    if not table_name:
                        logger.warning(f"表ID {table_id} 没有tableName，跳过")
                        continue
                    
                    # 获取字段列表
                    fields_data = self.api_client.get_table_fields(table_id)
                    if not fields_data:
                        logger.warning(f"表 {table_name} (ID={table_id}) 没有字段数据")
                        continue
                    
                    # 转换为MetadataField对象
                    converted_count = 0
                    for field_data in fields_data:
                        field = self._api_data_to_metadata_field(field_data, table_name)
                        if field:
                            all_fields.append(field)
                            converted_count += 1
                    
                    logger.info(f"从表 {table_name} (ID={table_id}) 成功转换 {converted_count}/{len(fields_data)} 个字段")
                    
                except Exception as e:
                    logger.error(f"处理表ID {table_id} 时失败: {e}")
                    continue
            
            logger.info(f"从API成功加载 {len(all_fields)} 个字段")
            return all_fields
            
        except Exception as e:
            logger.error(f"从API加载元数据失败: {e}")
            return []
        finally:
            if self.api_client:
                self.api_client.close()
    
    def _api_data_to_metadata_field(self, data: Dict[str, Any], table_name: str) -> Optional[MetadataField]:
        """
        将API返回的数据转换为MetadataField对象
        
        Args:
            data: API返回的字段数据
            table_name: 表名
            
        Returns:
            MetadataField对象
        """
        try:
            # 必需字段 - 处理None值
            column_name = (data.get('fieldName') or '').strip()
            chinese_name = (data.get('fieldDisplayName') or '').strip()
            
            if not column_name or not chinese_name:
                logger.warning(f"字段缺少必需信息: {data}")
                return None
            
            # 处理别名 - synonyms是逗号分隔的字符串，处理None值
            alias = []
            synonyms_str = (data.get('synonyms') or '').strip()
            if synonyms_str:
                # 按逗号分割
                alias = [s.strip() for s in synonyms_str.split(',') if s.strip()]
            
            # 处理描述 - 处理None值
            description = (data.get('fieldComment') or '').strip()
            
            # 数据类型 - 处理None值
            data_type = (data.get('dataType') or 'text').strip()
            
            # 字段类型 - 转换为大写，处理None值
            field_type_raw = data.get('fieldType')
            if field_type_raw:
                field_type = field_type_raw.strip().upper()
                if field_type not in ['DIMENSION', 'METRIC', 'ATTRIBUTE']:
                    logger.warning(f"未识别的字段类型 '{field_type}'，默认为 METRIC")
                    field_type = 'METRIC'
            else:
                # fieldType为None或空，默认为METRIC
                field_type = 'METRIC'
            
            # 是否有效
            is_effect = bool(data.get('isEnabled', True))
            
            # 数据格式 - 处理None值
            data_format = (data.get('displayFormat') or '').strip()
            
            # 创建MetadataField对象
            field = MetadataField(
                table_name=table_name,
                column_name=column_name,
                chinese_name=chinese_name,
                alias=alias,
                description=description,
                data_type=data_type,
                field_type=field_type,
                is_effect=is_effect,
                data_format=data_format,
                sample_data=None  # API中没有sample_data
            )
            
            return field
            
        except Exception as e:
            logger.error(f"转换API数据为MetadataField失败: {e}, 数据: {data}")
            return None
    
    def load_from_excel(self) -> List[MetadataField]:
        """从Excel文件加载元数据"""
        try:
            if not Path(self.excel_path).exists():
                logger.error(f"元数据文件不存在: {self.excel_path}")
                return []
            
            # 读取Excel文件
            df = pd.read_excel(self.excel_path)
            logger.info(f"从Excel文件读取 {len(df)} 条记录")
            
            # 转换为MetadataField对象
            fields = []
            for idx, row in df.iterrows():
                try:
                    field = self._row_to_metadata_field(row)
                    if field:
                        fields.append(field)
                except Exception as e:
                    logger.warning(f"转换第{idx+1}行数据失败: {e}")
                    continue
            
            logger.info(f"成功转换 {len(fields)} 个有效字段")
            return fields
            
        except Exception as e:
            logger.error(f"加载Excel文件失败: {e}")
            return []
    
    def _row_to_metadata_field(self, row: pd.Series) -> Optional[MetadataField]:
        """将DataFrame行转换为MetadataField对象"""
        try:
            # 处理必需字段 - 支持新旧字段名
            table_name = str(row.get('table_name', '')).strip()
            column_name = str(row.get('column_name', '')).strip()
            
            # 优先使用新字段名chinese_name，如果不存在则使用旧字段名display_name
            chinese_name = str(row.get('chinese_name', row.get('display_name', ''))).strip()
            
            # 跳过无效行
            if not table_name or not column_name or not chinese_name:
                return None
            
            # 过滤掉明显无效的数据
            if table_name == 'nan' or column_name == 'nan' or chinese_name == 'nan':
                return None
            
            # 处理别名 - 优先使用alias，如果不存在则使用旧的synonyms字段
            alias = []
            alias_str = str(row.get('alias', row.get('synonyms', ''))).strip()
            if alias_str and alias_str != 'nan':
                try:
                    # 尝试解析JSON格式的别名
                    if alias_str.startswith('[') and alias_str.endswith(']'):
                        alias = json.loads(alias_str)
                    elif alias_str.startswith('"[') and alias_str.endswith(']"'):
                        # 处理被双引号包围的JSON
                        clean_str = alias_str.strip('"')
                        alias = json.loads(clean_str)
                    else:
                        # 如果不是JSON格式，按逗号或分号分割
                        separators = [',', '，', ';', '；', '|']
                        for sep in separators:
                            if sep in alias_str:
                                alias = [s.strip() for s in alias_str.split(sep) if s.strip()]
                                break
                        else:
                            # 如果没有分隔符，作为单个别名
                            alias = [alias_str]
                except (json.JSONDecodeError, ValueError):
                    # JSON解析失败，尝试其他格式
                    alias = [s.strip() for s in alias_str.replace('，', ',').split(',') if s.strip()]
            
            # 清理别名列表
            alias = [a for a in alias if a and a != 'nan' and len(a.strip()) > 0]
            
            # 处理描述
            description = str(row.get('column_comment', '')).strip()
            if description == 'nan':
                description = ''
            
            # 处理数据类型
            data_type = str(row.get('data_type', 'text')).strip()
            if data_type == 'nan':
                data_type = 'text'
            
            # 处理字段类型 - 转换为大写
            field_type = str(row.get('field_type', '')).strip().upper()
            if field_type == 'NAN' or not field_type:
                # 向后兼容：如果没有field_type字段，默认为METRIC
                field_type = "METRIC"
            elif field_type not in ['DIMENSION', 'METRIC', 'ATTRIBUTE']:
                # 如果字段类型不是预期值，默认为METRIC
                logger.warning(f"未识别的字段类型 '{field_type}'，默认为 METRIC")
                field_type = 'METRIC'
            
            # 处理是否有效字段
            is_effect = self._parse_bool(row.get('is_effect', 1))
            
            # 处理数据格式
            data_format = str(row.get('data_format', '')).strip()
            if data_format == 'nan':
                data_format = ''
            
            # 处理示例数据
            sample_data = str(row.get('sample', '')).strip()
            if sample_data == 'nan':
                sample_data = None
            
            # 创建MetadataField对象
            field = MetadataField(
                table_name=table_name,
                column_name=column_name,
                chinese_name=chinese_name,
                alias=alias,
                description=description,
                data_type=data_type,
                field_type=field_type,
                is_effect=is_effect,
                data_format=data_format,
                sample_data=sample_data
            )
            
            return field
            
        except Exception as e:
            logger.error(f"转换行数据失败: {e}, 行数据: {row.to_dict()}")
            return None
    
    def _parse_bool(self, value: Any) -> bool:
        """解析布尔值"""
        if isinstance(value, bool):
            return value
        elif isinstance(value, (int, float)):
            return bool(value)
        elif isinstance(value, str):
            value = value.strip().lower()
            if value in ['1', 'true', 'yes', '是', 'y', 'on', 'enable', 'enabled']:
                return True
            elif value in ['0', 'false', 'no', '否', 'n', 'off', 'disable', 'disabled']:
                return False
        return False
    
    def validate_fields(self, fields: List[MetadataField]) -> Dict[str, Any]:
        """验证字段数据"""
        stats = {
            'total': len(fields),
            'valid': 0,
            'invalid': 0,
            'enabled_fields': 0,
            'tables': set(),
            'data_types': set(),
            'issues': []
        }
        
        for field in fields:
            try:
                # 基础验证
                if not field.table_name or not field.column_name or not field.chinese_name:
                    stats['invalid'] += 1
                    stats['issues'].append(f"字段缺少必需信息: {field.table_name}.{field.column_name}")
                    continue
                
                stats['valid'] += 1
                stats['tables'].add(field.table_name)
                stats['data_types'].add(field.data_type)
                
                if field.is_effect:
                    stats['enabled_fields'] += 1
                    
            except Exception as e:
                stats['invalid'] += 1
                stats['issues'].append(f"字段验证失败: {e}")
        
        # 转换set为list以便JSON序列化
        stats['tables'] = sorted(list(stats['tables']))
        stats['data_types'] = sorted(list(stats['data_types']))
        
        return stats
    
    def get_sample_data(self, limit: int = 5) -> List[Dict[str, Any]]:
        """获取样本数据用于预览"""
        try:
            fields = self.load_from_excel()
            if not fields:
                return []
            
            sample_fields = fields[:limit]
            sample_data = []
            
            for field in sample_fields:
                sample_data.append({
                    'table_name': field.table_name,
                    'column_name': field.column_name,
                    'chinese_name': field.chinese_name,
                    'alias': field.alias[:3] if field.alias else [],  # 只显示前3个别名
                    'description': field.description[:100] + '...' if len(field.description) > 100 else field.description,
                    'is_effect': field.is_effect,
                    'data_type': field.data_type
                })
            
            return sample_data
            
        except Exception as e:
            logger.error(f"获取样本数据失败: {e}")
            return []


# ==================== Metric数据加载器 ====================

class MetricLoader:
    """指标数据加载器"""
    
    def __init__(self, excel_path: Optional[str] = None, api_base_url: Optional[str] = None, jwt: Optional[str] = None):
        """初始化加载器"""
        self.excel_path = excel_path or config.metric_excel_full_path
        self.api_base_url = api_base_url or config.METADATA_API_BASE_URL
        self.jwt = jwt or config.METADATA_API_JWT
        self.api_client = None
    
    def load(self) -> List[Metric]:
        """
        智能加载指标 - 根据配置自动选择数据源
        
        Returns:
            指标对象列表
        """
        if config.API_SYNC_ENABLED:
            logger.info("从API加载指标（启用API同步模式）")
            return self.load_from_api()
        else:
            logger.info("从Excel加载指标（未启用API同步）")
            return self.load_from_excel()
    
    def load_from_api(self, max_workers: int = 10, ids: Optional[str] = None) -> List[Metric]:
        """
        从API加载指标数据
        
        Args:
            max_workers: 并行请求的最大工作线程数
            ids: 指标ID列表（逗号分隔的字符串），留空则使用环境变量配置
            
        Returns:
            指标对象列表
        """
        try:
            self.api_client = MetricAPIClient(self.api_base_url, jwt=self.jwt)
            
            # 如果没有传ids，使用环境变量
            metric_ids = ids or config.API_METRIC_IDS
            
            # 获取指标列表 - 传入ids参数
            metrics_list = self.api_client.get_metrics_list(ids=metric_ids)
            if not metrics_list:
                logger.warning("API返回的指标列表为空")
                return []
            
            logger.info(f"从API获取到 {len(metrics_list)} 个指标")
            
            # 并行获取每个指标的详情
            metrics = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_metric = {
                    executor.submit(self._load_single_metric, metric_data): metric_data
                    for metric_data in metrics_list
                }
                
                # 收集结果
                for future in as_completed(future_to_metric):
                    metric_data = future_to_metric[future]
                    try:
                        metric = future.result()
                        if metric:
                            metrics.append(metric)
                    except Exception as e:
                        logger.error(f"处理指标失败 (ID={metric_data.get('id')}): {e}")
            
            logger.info(f"成功从API加载 {len(metrics)} 个指标")
            return metrics
            
        except Exception as e:
            logger.error(f"从API加载指标失败: {e}")
            return []
        finally:
            if self.api_client:
                self.api_client.close()
    
    def _load_single_metric(self, metric_data: Dict[str, Any]) -> Optional[Metric]:
        """
        加载单个指标（包含详情）
        
        Args:
            metric_data: 指标基本信息
            
        Returns:
            Metric对象
        """
        try:
            metric_id = metric_data.get('id')
            if not metric_id:
                logger.warning(f"指标数据缺少ID: {metric_data}")
                return None
            
            # 获取指标详情
            detail = self.api_client.get_metric_detail(metric_id)
            if not detail:
                logger.warning(f"无法获取指标详情 (ID={metric_id})")
                # 即使没有详情，也尝试创建基本的Metric对象
                detail = {}
            
            # 转换为Metric对象
            return self._api_data_to_metric(metric_data, detail)
            
        except Exception as e:
            logger.error(f"加载单个指标失败: {e}")
            return None
    
    def _api_data_to_metric(self, basic_data: Dict[str, Any], detail_data: Dict[str, Any]) -> Optional[Metric]:
        """
        将API返回的数据转换为Metric对象
        
        Args:
            basic_data: 指标列表中的基本信息
            detail_data: 指标详情信息
            
        Returns:
            Metric对象
        """
        try:
            # 必需字段
            metric_id = basic_data.get('id')
            metric_name = (basic_data.get('name') or '').strip()
            
            if not metric_id or not metric_name:
                logger.warning(f"指标缺少必需信息: {basic_data}")
                return None
            
            # 别名列表 - 处理None值
            metric_alias = basic_data.get('metricAlias') or []
            if not isinstance(metric_alias, list):
                metric_alias = []
            
            # 相关实体列表 - 处理None值
            related_entities = basic_data.get('relatedEntities') or []
            if not isinstance(related_entities, list):
                related_entities = []
            
            # 业务定义 - 从code字段获取并添加{}包裹，处理None值
            code = (basic_data.get('code') or '').strip()
            business_definition = f"{{{code}}}" if code else ""
            
            # 从详情中获取SQL和依赖表 - 处理None值
            metric_sql = (detail_data.get('calculationLogicExplanation') or '').strip()
            
            # depends_on_tables和depends_on_columns都从tables字段获取
            tables = detail_data.get('tables', [])
            if not isinstance(tables, list):
                tables = []
            
            depends_on_tables = tables
            depends_on_columns = tables  # 根据用户要求，depends_on_columns也对应tables
            
            # 创建Metric对象
            metric = Metric(
                metric_id=metric_id,
                metric_name=metric_name,
                metric_alias=metric_alias,
                related_entities=related_entities,
                metric_sql=metric_sql,
                depends_on_tables=depends_on_tables,
                depends_on_columns=depends_on_columns,
                business_definition=business_definition
            )
            
            return metric
            
        except Exception as e:
            logger.error(f"转换API数据为Metric失败: {e}, 数据: {basic_data}")
            return None
    
    def load_from_excel(self) -> List[Metric]:
        """从Excel文件加载指标数据"""
        try:
            if not Path(self.excel_path).exists():
                logger.error(f"指标文件不存在: {self.excel_path}")
                return []
            
            # 读取Excel文件
            df = pd.read_excel(self.excel_path)
            logger.info(f"从Excel文件读取 {len(df)} 条指标记录")
            
            # 转换为Metric对象
            metrics = []
            for idx, row in df.iterrows():
                try:
                    metric = self._row_to_metric(row)
                    if metric:
                        metrics.append(metric)
                except Exception as e:
                    logger.warning(f"转换第{idx+1}行指标数据失败: {e}")
                    continue
            
            logger.info(f"成功转换 {len(metrics)} 个有效指标")
            return metrics
            
        except Exception as e:
            logger.error(f"加载指标Excel文件失败: {e}")
            return []
    
    def _row_to_metric(self, row: pd.Series) -> Optional[Metric]:
        """将DataFrame行转换为Metric对象"""
        try:
            # 处理必需字段
            metric_id = row.get('metric_id')
            metric_name = str(row.get('metric_name', '')).strip()
            
            # 跳过无效行
            if pd.isna(metric_id) or not metric_name or metric_name == 'nan':
                return None
            
            # 转换metric_id为整数
            try:
                metric_id = int(metric_id)
            except (ValueError, TypeError):
                logger.warning(f"无效的metric_id: {metric_id}")
                return None
            
            # 处理别名 - JSON数组字段
            metric_alias = self._parse_json_array(row.get('metric_alias', ''))
            
            # 处理相关实体 - JSON数组字段
            related_entities = self._parse_json_array(row.get('related_entities', ''))
            
            # 处理SQL
            metric_sql = str(row.get('metric_sql', '')).strip()
            if metric_sql == 'nan':
                metric_sql = ''
            
            # 处理依赖的表 - JSON数组字段
            depends_on_tables = self._parse_json_array(row.get('depends_on_tables', ''))
            
            # 处理依赖的字段 - JSON数组字段
            depends_on_columns = self._parse_json_array(row.get('depends_on_columns', ''))
            
            # 处理业务定义
            business_definition = str(row.get('business_definition', '')).strip()
            if business_definition == 'nan':
                business_definition = ''
            
            # 创建Metric对象
            metric = Metric(
                metric_id=metric_id,
                metric_name=metric_name,
                metric_alias=metric_alias,
                related_entities=related_entities,
                metric_sql=metric_sql,
                depends_on_tables=depends_on_tables,
                depends_on_columns=depends_on_columns,
                business_definition=business_definition
            )
            
            return metric
            
        except Exception as e:
            logger.error(f"转换行数据为Metric失败: {e}, 行数据: {row.to_dict()}")
            return None
    
    def _parse_json_array(self, value: Any) -> List[str]:
        """解析JSON数组字段"""
        if pd.isna(value):
            return []
        
        value_str = str(value).strip()
        if not value_str or value_str == 'nan':
            return []
        
        try:
            # 尝试解析JSON格式
            if value_str.startswith('[') and value_str.endswith(']'):
                parsed = json.loads(value_str)
                if isinstance(parsed, list):
                    # 确保所有元素都是字符串
                    return [str(item).strip() for item in parsed if item and str(item).strip() != 'nan']
            elif value_str.startswith('"[') and value_str.endswith(']"'):
                # 处理被双引号包围的JSON
                clean_str = value_str.strip('"')
                parsed = json.loads(clean_str)
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if item and str(item).strip() != 'nan']
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug(f"JSON解析失败，尝试其他格式: {e}")
        
        # 如果不是JSON格式，按逗号分割
        separators = [',', '，', ';', '；', '|']
        for sep in separators:
            if sep in value_str:
                items = [s.strip() for s in value_str.split(sep) if s.strip() and s.strip() != 'nan']
                if items:
                    return items
        
        # 作为单个元素
        return [value_str] if value_str and value_str != 'nan' else []
    
    def validate_metrics(self, metrics: List[Metric]) -> Dict[str, Any]:
        """验证指标数据"""
        stats = {
            'total': len(metrics),
            'valid': 0,
            'invalid': 0,
            'issues': []
        }
        
        for metric in metrics:
            try:
                # 基础验证
                if not metric.metric_name or metric.metric_id <= 0:
                    stats['invalid'] += 1
                    stats['issues'].append(f"指标缺少必需信息: ID={metric.metric_id}, Name={metric.metric_name}")
                    continue
                
                stats['valid'] += 1
                    
            except Exception as e:
                stats['invalid'] += 1
                stats['issues'].append(f"指标验证失败: {e}")
        
        return stats
    
    def get_sample_data(self, limit: int = 5) -> List[Dict[str, Any]]:
        """获取样本数据用于预览"""
        try:
            metrics = self.load_from_excel()
            if not metrics:
                return []
            
            sample_metrics = metrics[:limit]
            sample_data = []
            
            for metric in sample_metrics:
                sample_data.append({
                    'metric_id': metric.metric_id,
                    'metric_name': metric.metric_name,
                    'metric_alias': metric.metric_alias[:3] if metric.metric_alias else [],
                    'business_definition': metric.business_definition[:100] + '...' if len(metric.business_definition) > 100 else metric.business_definition,
                    'depends_on_tables': metric.depends_on_tables[:3] if metric.depends_on_tables else []
                })
            
            return sample_data
            
        except Exception as e:
            logger.error(f"获取指标样本数据失败: {e}")
            return [] 