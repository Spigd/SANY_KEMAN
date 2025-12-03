"""
配置管理
"""

import os
import json
from pathlib import Path
from typing import Optional, Dict, Any

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

class Config:
    """配置管理类"""
    
    def __init__(self):
        """初始化配置"""
        # 项目根目录
        self.PROJECT_ROOT = Path(__file__).parent.parent
        
        # 加载.env文件（如果存在且dotenv可用）
        if DOTENV_AVAILABLE:
            env_file = self.PROJECT_ROOT / '.env'
            if env_file.exists():
                load_dotenv(env_file)
            # 也尝试加载config.env（向后兼容）
            config_env_file = self.PROJECT_ROOT / 'config.env'
            if config_env_file.exists():
                load_dotenv(config_env_file, override=True)
        
        # Elasticsearch配置
        self.ES_HOST = os.getenv('ES_HOST', '10.66.0.160')
        self.ES_PORT = int(os.getenv('ES_PORT', '9200'))
        self.ES_INDEX_PREFIX = os.getenv('ES_INDEX_PREFIX', 'kman')
        
        # API配置
        self.API_HOST = os.getenv('API_HOST', '0.0.0.0')
        self.API_PORT = int(os.getenv('API_PORT', '8083'))
        
        # 数据文件配置
        self.METADATA_EXCEL_PATH = os.getenv('METADATA_EXCEL_PATH', '客满-元数据表.xlsx')
        self.METRIC_EXCEL_PATH = os.getenv('METRIC_EXCEL_PATH', 'metric_latest.xlsx')
        
        # 元数据和指标API配置
        self.METADATA_API_BASE_URL = os.getenv('METADATA_API_BASE_URL', 'https://metric-asset-api-internal.rootcloudapp.com')
        self.METADATA_API_TIMEOUT = int(os.getenv('METADATA_API_TIMEOUT', '30'))
        self.METADATA_API_JWT = os.getenv('METADATA_API_JWT', 'eyJhbGciOiJIUzUxMiJ9.eyJvcmdhbml6YXRpb25JZCI6OTAwMCwibmFtZSI6ImFkbWluIiwiaWQiOi0xLCJpc0FkbWluIjp0cnVlLCJ1c2VybmFtZSI6ImFkbWluIiwic3ViIjoiYWRtaW4iLCJpYXQiOjE3NjQ2Njc3MTQsImV4cCI6MTc2NDc1NDExNH0.n_AYqqlh4JxrA2QD1oxqN9Zo1kitiv_wUKjdYH76kP-d34JxasKB4DXmUkNxa6pHxXGzRyko9JAAuMUWt4Ju_Q')
        
        # API数据同步配置
        self.API_SYNC_ENABLED = os.getenv('API_SYNC_ENABLED', 'true').lower() == 'true'
        self.API_SYNC_INTERVAL = float(os.getenv('API_SYNC_INTERVAL', ''))  # 小时
        self.API_TABLE_IDS = os.getenv('API_TABLE_IDS', '268,269,270')  # 逗号分隔的表ID列表
        self.API_METRIC_IDS = os.getenv('API_METRIC_IDS', '171,172,357')  # 逗号分隔的指标ID列表，如 "171,172"
        
        # 混合搜索配置
        self.HYBRID_SEARCH_WEIGHTS = {
            'elasticsearch': float(os.getenv('ES_WEIGHT', '1.0')),
            'ac_matcher': float(os.getenv('AC_WEIGHT', '0.9')),
            'similarity': float(os.getenv('SIM_WEIGHT', '0.8'))
        }
        
        # 分词器配置
        self.DEFAULT_TOKENIZER = os.getenv('DEFAULT_TOKENIZER', 'ik_max_word')
        self.DEFAULT_SEARCH_ANALYZER = os.getenv('DEFAULT_SEARCH_ANALYZER', 'ik_smart')
        
        # 日志配置
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        
        # 数据库配置 - 支持多个数据源
        self.DATABASE_CONFIGS = self._load_database_configs()
        
        # 维度值索引配置
        self.DIMENSION_VALUE_INDEXING = {
            'enabled': os.getenv('DIMENSION_VALUE_INDEXING_ENABLED', 'true').lower() == 'true',
            'max_values_per_column': int(os.getenv('MAX_VALUES_PER_COLUMN', '1000')),
            'batch_size': int(os.getenv('DIMENSION_BATCH_SIZE', '100')),
            'auto_extract_on_index': os.getenv('AUTO_EXTRACT_DIMENSIONS', 'true').lower() == 'true'
        }
    
    def _load_database_configs(self) -> Dict[str, Dict[str, Any]]:
        """加载数据库配置"""
        configs = {}
        
        # 从环境变量加载默认数据库配置
        default_db_config = {
            'type': os.getenv('DB_TYPE', 'mysql'),
            'host': os.getenv('DB_HOST', '10.70.40.134'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'Kb3LCNsM2Stp!d'),
            'database': os.getenv('DB_DATABASE', 'keman_data2'),
            'charset': os.getenv('DB_CHARSET', 'utf8mb4')
        }
        
        # 如果配置了数据库连接信息，添加默认配置
        if default_db_config['user'] and default_db_config['database']:
            configs['default'] = default_db_config
        
        # 从JSON配置文件加载多个数据库配置
        db_configs_json = os.getenv('DATABASE_CONFIGS_JSON', '')
        if db_configs_json:
            try:
                additional_configs = json.loads(db_configs_json)
                configs.update(additional_configs)
            except json.JSONDecodeError:
                pass
        
        # 从配置文件加载
        config_file = self.PROJECT_ROOT / 'database_configs.json'
        if config_file.exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    file_configs = json.load(f)
                    configs.update(file_configs)
            except (json.JSONDecodeError, IOError):
                pass
        
        return configs
    
    @property
    def elasticsearch_url(self) -> str:
        """Elasticsearch URL"""
        return f"http://{self.ES_HOST}:{self.ES_PORT}"
    
    @property
    def metadata_index_name(self) -> str:
        """元数据字段索引名称"""
        return f"{self.ES_INDEX_PREFIX}_fields"
    
    @property
    def dimension_values_index_name(self) -> str:
        """维度值索引名称"""
        return f"{self.ES_INDEX_PREFIX}_dimension_values"
    
    @property
    def metric_index_name(self) -> str:
        """指标索引名称"""
        return f"{self.ES_INDEX_PREFIX}_metrics"
    
    @property
    def metadata_excel_full_path(self) -> str:
        """元数据Excel文件完整路径"""
        if os.path.isabs(self.METADATA_EXCEL_PATH):
            return self.METADATA_EXCEL_PATH
        return str(self.PROJECT_ROOT / self.METADATA_EXCEL_PATH)
    
    @property
    def metric_excel_full_path(self) -> str:
        """指标Excel文件完整路径"""
        if os.path.isabs(self.METRIC_EXCEL_PATH):
            return self.METRIC_EXCEL_PATH
        return str(self.PROJECT_ROOT / self.METRIC_EXCEL_PATH)
    
    def get_database_config(self, name: str = 'default') -> Optional[Dict[str, Any]]:
        """获取指定名称的数据库配置"""
        return self.DATABASE_CONFIGS.get(name)
    
    def add_database_config(self, name: str, config: Dict[str, Any]):
        """添加数据库配置"""
        self.DATABASE_CONFIGS[name] = config
    
    def save_database_configs(self):
        """保存数据库配置到文件"""
        config_file = self.PROJECT_ROOT / 'database_configs.json'
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(self.DATABASE_CONFIGS, f, indent=2, ensure_ascii=False)
        except IOError as e:
            raise Exception(f"保存数据库配置失败: {e}")
    
    def is_dimension_indexing_enabled(self) -> bool:
        """检查是否启用维度值索引"""
        return self.DIMENSION_VALUE_INDEXING['enabled']

# 全局配置实例
config = Config() 