"""
数据库连接抽象层 - 支持MySQL和PostgreSQL
"""

import logging
import hashlib
from typing import List, Dict, Any, Optional, Union, Tuple
from abc import ABC, abstractmethod
from urllib.parse import quote_plus
from datetime import datetime

try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import psycopg2
    import psycopg2.extras
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False

from core.models import DimensionValue

logger = logging.getLogger(__name__)


class DatabaseConnection(ABC):
    """数据库连接抽象基类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.db_type = config.get('type', '').lower()
    
    @abstractmethod
    def connect(self) -> bool:
        """连接数据库"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """断开数据库连接"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """执行查询并返回结果"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """测试数据库连接"""
        pass
    
    def get_distinct_values(self, table_name: str, column_name: str, 
                          limit: Optional[int] = None) -> List[Tuple[str, int]]:
        """
        获取指定表列的DISTINCT值及其频次
        
        Args:
            table_name: 表名
            column_name: 列名
            limit: 限制返回数量
            
        Returns:
            List[(value, frequency), ...]
        """
        try:
            # 构建查询语句
            query = f"""
                SELECT {column_name} as value, COUNT(*) as frequency
                FROM {table_name}
                WHERE {column_name} IS NOT NULL 
                  AND {column_name} != ''
                GROUP BY {column_name}
                ORDER BY frequency DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            results = self.execute_query(query)
            return [(row['value'], row['frequency']) for row in results]
            
        except Exception as e:
            logger.error(f"获取维度值失败 {table_name}.{column_name}: {e}")
            return []
    
    def validate_table_column(self, table_name: str, column_name: str) -> bool:
        """验证表和列是否存在"""
        try:
            # 尝试查询一行数据来验证表和列存在
            query = f"SELECT {column_name} FROM {table_name} LIMIT 1"
            self.execute_query(query)
            return True
        except Exception:
            return False


class MySQLConnection(DatabaseConnection):
    """MySQL数据库连接"""
    
    def __init__(self, config: Dict[str, Any]):
        if not MYSQL_AVAILABLE:
            raise ImportError("PyMySQL未安装，无法连接MySQL数据库")
        super().__init__(config)
    
    def connect(self) -> bool:
        """连接MySQL数据库"""
        try:
            self.connection = pymysql.connect(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 3306),
                user=self.config.get('user', ''),
                password=self.config.get('password', ''),
                database=self.config.get('database', ''),
                charset=self.config.get('charset', 'utf8mb4'),
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info("MySQL连接成功")
            return True
        except Exception as e:
            logger.error(f"MySQL连接失败: {e}")
            return False
    
    def disconnect(self):
        """断开MySQL连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """执行MySQL查询"""
        if not self.connection:
            raise Exception("数据库未连接")
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"MySQL查询失败: {query}, 错误: {e}")
            raise
    
    def test_connection(self) -> bool:
        """测试MySQL连接"""
        try:
            if not self.connection:
                return False
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception:
            return False


class PostgreSQLConnection(DatabaseConnection):
    """PostgreSQL数据库连接"""
    
    def __init__(self, config: Dict[str, Any]):
        if not POSTGRESQL_AVAILABLE:
            raise ImportError("psycopg2未安装，无法连接PostgreSQL数据库")
        super().__init__(config)
    
    def connect(self) -> bool:
        """连接PostgreSQL数据库"""
        try:
            connection_string = (
                f"host={self.config.get('host', 'localhost')} "
                f"port={self.config.get('port', 5432)} "
                f"dbname={self.config.get('database', '')} "
                f"user={self.config.get('user', '')} "
                f"password={self.config.get('password', '')}"
            )
            
            self.connection = psycopg2.connect(connection_string)
            self.connection.autocommit = True
            logger.info("PostgreSQL连接成功")
            return True
        except Exception as e:
            logger.error(f"PostgreSQL连接失败: {e}")
            return False
    
    def disconnect(self):
        """断开PostgreSQL连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """执行PostgreSQL查询"""
        if not self.connection:
            raise Exception("数据库未连接")
        
        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                results = cursor.fetchall()
                # 转换为普通字典列表
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"PostgreSQL查询失败: {query}, 错误: {e}")
            raise
    
    def test_connection(self) -> bool:
        """测试PostgreSQL连接"""
        try:
            if not self.connection:
                return False
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception:
            return False


class DatabaseManager:
    """数据库管理器 - 工厂模式创建数据库连接"""
    
    @staticmethod
    def create_connection(config: Dict[str, Any]) -> DatabaseConnection:
        """根据配置创建数据库连接"""
        db_type = config.get('type', '').lower()
        
        if db_type == 'mysql':
            return MySQLConnection(config)
        elif db_type == 'postgresql':
            return PostgreSQLConnection(config)
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")
    
    @staticmethod
    def test_database_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """测试数据库配置"""
        try:
            conn = DatabaseManager.create_connection(config)
            if conn.connect():
                is_connected = conn.test_connection()
                conn.disconnect()
                return {
                    'success': True,
                    'message': f"{config.get('type')} 数据库连接成功",
                    'connected': is_connected
                }
            else:
                return {
                    'success': False,
                    'message': f"{config.get('type')} 数据库连接失败"
                }
        except Exception as e:
            return {
                'success': False,
                'message': f"数据库配置测试失败: {str(e)}"
            }


class DimensionExtractor:
    """维度值提取器"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
    
    def extract_dimension_values(self, table_name: str, column_name: str, 
                               chinese_name: str, limit: Optional[int] = 1000) -> List[DimensionValue]:
        """
        从数据库提取维度值
        
        Args:
            table_name: 表名
            column_name: 列名
            chinese_name: 中文名称
            limit: 限制提取数量
            
        Returns:
            维度值列表
        """
        try:
            # 验证表和列存在
            if not self.db_connection.validate_table_column(table_name, column_name):
                logger.warning(f"表或列不存在: {table_name}.{column_name}")
                return []
            
            # 获取distinct值
            values_with_freq = self.db_connection.get_distinct_values(
                table_name, column_name, limit
            )
            
            dimension_values = []
            for value, frequency in values_with_freq:
                if value is None or str(value).strip() == '':
                    continue
                
                value_str = str(value).strip()
                value_hash = hashlib.md5(
                    f"{table_name}_{column_name}_{value_str}".encode('utf-8')
                ).hexdigest()
                
                dimension_value = DimensionValue(
                    table_name=table_name,
                    column_name=column_name,
                    chinese_name=chinese_name,
                    value=value_str,
                    value_hash=value_hash,
                    frequency=frequency,
                    created_at=datetime.now()
                )
                
                dimension_values.append(dimension_value)
            
            logger.info(f"从 {table_name}.{column_name} 提取了 {len(dimension_values)} 个维度值")
            return dimension_values
            
        except Exception as e:
            logger.error(f"提取维度值失败 {table_name}.{column_name}: {e}")
            return []
    
    def extract_all_dimensions(self, metadata_fields: List['MetadataField'], 
                             limit_per_column: Optional[int] = 1000) -> List[DimensionValue]:
        """
        批量提取所有维度字段的值
        
        Args:
            metadata_fields: 元数据字段列表
            limit_per_column: 每列限制提取数量
            
        Returns:
            所有维度值列表
        """
        all_dimension_values = []
        dimension_fields = [f for f in metadata_fields if f.field_type == 'dimension']
        
        logger.info(f"开始提取 {len(dimension_fields)} 个维度字段的值...")
        logger.info(f"开始提取【{dimension_fields}】维度值的数据")
        
        for field in dimension_fields:
            dimension_values = self.extract_dimension_values(
                field.table_name,
                field.column_name,
                field.chinese_name,
                limit_per_column
            )
            all_dimension_values.extend(dimension_values)
        
        logger.info(f"总共提取了 {len(all_dimension_values)} 个维度值")
        return all_dimension_values 