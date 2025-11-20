"""
元数据表数据加载器 - V3增强版
包含MetadataLoader和MetricLoader
"""

import json
import logging
import pandas as pd
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime

from core.config import config
from core.models import MetadataField, Metric

logger = logging.getLogger(__name__)


class MetadataLoader:
    """元数据表加载器"""
    
    def __init__(self, excel_path: Optional[str] = None):
        """初始化加载器"""
        self.excel_path = excel_path or config.metadata_excel_full_path
    
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
            
            # 处理字段类型 - 新增支持，向后兼容
            field_type = str(row.get('field_type', '')).strip().lower()
            if field_type == 'nan' or not field_type:
                # 向后兼容：如果没有field_type字段，根据其他信息推断
                field_type = "metric"
            elif field_type not in ['dimension', 'metric']:
                # 如果字段类型不是预期值，默认为metric
                logger.warning(f"未识别的字段类型 '{field_type}'，默认为 metric")
                field_type = 'metric'
            
            # 处理布尔字段
            is_entity = self._parse_bool(row.get('is_entity', 0))
            is_enabled = self._parse_bool(row.get('is_effect', 1))
            is_enum = self._parse_bool(row.get('is_enum', 0))
            
            # 处理枚举值
            enum_values = {}
            enum_str = str(row.get('enum_value', '')).strip()
            if enum_str and enum_str != 'nan':
                try:
                    # 尝试解析JSON格式的枚举值
                    if enum_str.startswith('{') and enum_str.endswith('}'):
                        enum_values = json.loads(enum_str)
                    elif enum_str.startswith('"{') and enum_str.endswith('}"'):
                        # 处理被双引号包围的JSON
                        clean_str = enum_str.strip('"')
                        enum_values = json.loads(clean_str)
                    else:
                        # 尝试解析key:value格式
                        pairs = enum_str.replace('，', ',').split(',')
                        for pair in pairs:
                            if ':' in pair:
                                key, value = pair.split(':', 1)
                                enum_values[key.strip()] = value.strip()
                except (json.JSONDecodeError, ValueError):
                    logger.warning(f"枚举值解析失败: {enum_str}")
            
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
                is_entity=is_entity,
                is_enabled=is_enabled,
                is_enum=is_enum,
                enum_values=enum_values,
                sample_data=sample_data
            )
            
            return field
            
        except Exception as e:
            logger.error(f"转换行数据失败: {e}, 行数据: {row.to_dict()}")
            return None
    
    def _infer_field_type(self, row: pd.Series, data_type: str) -> str:
        """
        推断字段类型 - 向后兼容逻辑
        
        Args:
            row: DataFrame行数据
            data_type: 数据类型
            
        Returns:
            推断的字段类型：dimension 或 metric
        """
        # 推断规则：
        # 1. 如果是枚举类型，通常是维度
        # 2. 如果数据类型是text且有枚举值，可能是维度
        # 3. 如果字段名包含特定关键词，可能是维度
        # 4. 默认为metric
        
        is_enum = self._parse_bool(row.get('is_enum', 0))
        if is_enum:
            return 'dimension'
        
        # 检查是否有枚举值定义
        enum_str = str(row.get('enum_value', '')).strip()
        if enum_str and enum_str != 'nan':
            return 'dimension'
        
        # 根据字段名推断
        column_name = str(row.get('column_name', '')).strip().lower()
        chinese_name = str(row.get('chinese_name', row.get('display_name', ''))).strip().lower()
        
        # 常见维度字段关键词
        dimension_keywords = [
            'status', '状态', 'type', '类型', 'category', '分类', '类别',
            'level', '等级', 'grade', '级别', 'region', '地区', '区域',
            'department', '部门', 'team', '团队', 'group', '分组',
            'channel', '渠道', 'source', '来源', 'platform', '平台'
        ]
        
        for keyword in dimension_keywords:
            if keyword in column_name or keyword in chinese_name:
                return 'dimension'
        
        return 'metric'
    
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
            'entity_fields': 0,
            'enabled_fields': 0,
            'enum_fields': 0,
            'tables': set(),
            'data_types': set(),
            'issues': []
        }
        
        for field in fields:
            try:
                # 基础验证
                if not field.table_name or not field.column_name or not field.display_name:
                    stats['invalid'] += 1
                    stats['issues'].append(f"字段缺少必需信息: {field.table_name}.{field.column_name}")
                    continue
                
                stats['valid'] += 1
                stats['tables'].add(field.table_name)
                stats['data_types'].add(field.data_type)
                
                if field.is_entity:
                    stats['entity_fields'] += 1
                
                if field.is_enabled:
                    stats['enabled_fields'] += 1
                
                if field.is_enum:
                    stats['enum_fields'] += 1
                    
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
                    'is_entity': field.is_entity,
                    'is_enabled': field.is_enabled,
                    'data_type': field.data_type
                })
            
            return sample_data
            
        except Exception as e:
            logger.error(f"获取样本数据失败: {e}")
            return []


# ==================== Metric数据加载器 ====================

class MetricLoader:
    """指标数据加载器"""
    
    def __init__(self, excel_path: Optional[str] = None):
        """初始化加载器"""
        self.excel_path = excel_path or config.metric_excel_full_path
    
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
            
            # 处理指标类型
            metric_type = str(row.get('metric_type', '')).strip().lower()
            if metric_type == 'nan':
                metric_type = ''
            
            # 处理状态
            status = str(row.get('status', 'active')).strip().lower()
            if status == 'nan':
                status = 'active'
            
            # 处理负责人
            owner = str(row.get('owner', '')).strip()
            if owner == 'nan' or not owner:
                owner = None
            
            # 处理时间字段
            created_at = self._parse_datetime(row.get('created_at'))
            updated_at = self._parse_datetime(row.get('updated_at'))
            
            # 创建Metric对象
            metric = Metric(
                metric_id=metric_id,
                metric_name=metric_name,
                metric_alias=metric_alias,
                related_entities=related_entities,
                metric_sql=metric_sql,
                depends_on_tables=depends_on_tables,
                depends_on_columns=depends_on_columns,
                business_definition=business_definition,
                metric_type=metric_type,
                status=status,
                owner=owner,
                created_at=created_at,
                updated_at=updated_at
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
    
    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        """解析日期时间字段"""
        if pd.isna(value):
            return None
        
        # 如果已经是datetime对象
        if isinstance(value, datetime):
            return value
        
        # 如果是pandas的Timestamp
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        
        # 尝试解析字符串
        value_str = str(value).strip()
        if not value_str or value_str == 'nan':
            return None
        
        try:
            return pd.to_datetime(value_str).to_pydatetime()
        except Exception:
            return None
    
    def validate_metrics(self, metrics: List[Metric]) -> Dict[str, Any]:
        """验证指标数据"""
        stats = {
            'total': len(metrics),
            'valid': 0,
            'invalid': 0,
            'active_metrics': 0,
            'inactive_metrics': 0,
            'metric_types': {},
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
                
                # 统计状态
                if metric.status == 'active':
                    stats['active_metrics'] += 1
                else:
                    stats['inactive_metrics'] += 1
                
                # 统计指标类型
                if metric.metric_type:
                    stats['metric_types'][metric.metric_type] = stats['metric_types'].get(metric.metric_type, 0) + 1
                    
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
                    'metric_type': metric.metric_type,
                    'status': metric.status,
                    'business_definition': metric.business_definition[:100] + '...' if len(metric.business_definition) > 100 else metric.business_definition,
                    'depends_on_tables': metric.depends_on_tables[:3] if metric.depends_on_tables else []
                })
            
            return sample_data
            
        except Exception as e:
            logger.error(f"获取指标样本数据失败: {e}")
            return [] 