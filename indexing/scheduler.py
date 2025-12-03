"""
数据同步调度器 - 定时从API同步数据到Elasticsearch
"""

import logging
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, TYPE_CHECKING
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from core.config import config
from core.models import MetadataField
from indexing.data_loader import MetadataLoader, MetricLoader

if TYPE_CHECKING:
    from search.hybrid_searcher import HybridSearcher

logger = logging.getLogger(__name__)


class DataSyncScheduler:
    """数据同步调度器"""
    
    def __init__(self, hybrid_searcher: 'HybridSearcher'):
        """
        初始化调度器
        
        Args:
            hybrid_searcher: 混合搜索器实例
        """
        self.hybrid_searcher = hybrid_searcher
        self.scheduler = BackgroundScheduler()
        
        # 判断同步模式
        interval = config.API_SYNC_INTERVAL
        if interval and interval > 0:
            # 间隔模式
            self.sync_mode = 'interval'
            self.sync_interval_hours = interval
            self.sync_cron_time = None
        else:
            # 定时模式（每天早上5点）
            self.sync_mode = 'cron'
            self.sync_interval_hours = None
            self.sync_cron_time = '05:00'
        
        self.table_ids = self._parse_table_ids()
        self.sync_lock = threading.Lock()
        self.last_sync_time = None
        self.last_sync_status = None
        self.is_syncing = False
        
        if self.sync_mode == 'interval':
            logger.info(f"数据同步调度器初始化: 模式=间隔同步, 间隔={self.sync_interval_hours}小时, 表ID={self.table_ids}")
        else:
            logger.info(f"数据同步调度器初始化: 模式=定时同步, 时间={self.sync_cron_time}, 表ID={self.table_ids}")
    
    def _parse_table_ids(self) -> List[int]:
        """解析表ID列表"""
        table_ids_str = config.API_TABLE_IDS.strip()
        if not table_ids_str:
            logger.warning("未配置API_TABLE_IDS，无法从API同步元数据")
            return []
        
        try:
            table_ids = [int(tid.strip()) for tid in table_ids_str.split(',') if tid.strip()]
            logger.info(f"解析到 {len(table_ids)} 个表ID: {table_ids}")
            return table_ids
        except ValueError as e:
            logger.error(f"解析表ID列表失败: {e}")
            return []
    
    def start(self):
        """启动调度器"""
        if not config.API_SYNC_ENABLED:
            logger.info("API数据同步未启用（API_SYNC_ENABLED=false）")
            return
        
        if not self.table_ids:
            logger.warning("表ID列表为空，跳过启动数据同步调度器")
            return
        
        try:
            # 不在启动时立即同步，避免与手动同步冲突
            # 用户可以通过手动API触发首次同步，或等待定时任务执行
            
            # 根据模式设置不同的触发器
            if self.sync_mode == 'interval':
                # 间隔模式：按小时间隔执行
                trigger = IntervalTrigger(hours=self.sync_interval_hours)
                logger.info(f"✅ 使用间隔触发器，每 {self.sync_interval_hours} 小时执行一次")
            else:
                # 定时模式：每天固定时间执行
                from apscheduler.triggers.cron import CronTrigger
                hour, minute = self.sync_cron_time.split(':')
                trigger = CronTrigger(hour=int(hour), minute=int(minute))
                logger.info(f"✅ 使用定时触发器，每天 {self.sync_cron_time} 执行")
            
            self.scheduler.add_job(
                self.sync_data,
                trigger=trigger,
                id='data_sync_job',
                name='数据同步任务',
                replace_existing=True
            )
            
            self.scheduler.start()
            
            if self.sync_mode == 'interval':
                logger.info(f"✅ 数据同步调度器已启动，每 {self.sync_interval_hours} 小时自动执行同步")
            else:
                logger.info(f"✅ 数据同步调度器已启动，每天 {self.sync_cron_time} 自动执行同步")
            
        except Exception as e:
            logger.error(f"启动调度器失败: {e}")
    
    def stop(self):
        """停止调度器"""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
                logger.info("数据同步调度器已停止")
        except Exception as e:
            logger.error(f"停止调度器失败: {e}")
    
    def sync_data(self, force: bool = False) -> Dict[str, Any]:
        """
        同步数据（元数据 + 指标）
        
        Args:
            force: 是否强制同步（忽略锁检查）
            
        Returns:
            同步结果字典
        """
        # 检查是否正在同步
        if self.is_syncing and not force:
            logger.warning("数据同步正在进行中，跳过本次同步")
            return {
                'success': False,
                'message': '同步正在进行中',
                'timestamp': datetime.now().isoformat()
            }
        
        # 获取锁
        if not self.sync_lock.acquire(blocking=False):
            logger.warning("无法获取同步锁，跳过本次同步")
            return {
                'success': False,
                'message': '无法获取同步锁',
                'timestamp': datetime.now().isoformat()
            }
        
        try:
            self.is_syncing = True
            start_time = time.time()
            logger.info("=" * 60)
            logger.info("开始数据同步...")
            
            results = {
                'metadata': None,
                'dimension_values': None,
                'metrics': None,
                'success': True,
                'start_time': datetime.now().isoformat(),
                'errors': []
            }
            
            # 同步元数据
            try:
                logger.info("步骤 1/3: 同步元数据...")
                metadata_result = self._sync_metadata()
                results['metadata'] = metadata_result
                if not metadata_result.get('success'):
                    results['success'] = False
                    results['errors'].append(f"元数据同步失败: {metadata_result.get('message')}")
                else:
                    # 元数据同步成功后，立即同步维度值
                    try:
                        logger.info("步骤 2/3: 同步维度值...")
                        # 从 metadata_result 获取已加载的字段，避免重复加载
                        fields = metadata_result.get('fields', [])
                        if not fields:
                            # 如果没有返回字段，则重新加载
                            logger.warning("  未从元数据同步结果中获取字段，尝试重新加载...")
                            loader = MetadataLoader(jwt=config.METADATA_API_JWT)
                            fields = loader.load()
                        
                        dimension_values_result = self._sync_dimension_values(fields)
                        results['dimension_values'] = dimension_values_result
                        if not dimension_values_result.get('success'):
                            results['success'] = False
                            results['errors'].append(f"维度值同步失败: {dimension_values_result.get('message')}")
                    except Exception as e:
                        logger.error(f"维度值同步异常: {e}", exc_info=True)
                        results['success'] = False
                        results['errors'].append(f"维度值同步异常: {str(e)}")
                        results['dimension_values'] = {'success': False, 'message': str(e)}
            except Exception as e:
                logger.error(f"元数据同步异常: {e}", exc_info=True)
                results['success'] = False
                results['errors'].append(f"元数据同步异常: {str(e)}")
                results['metadata'] = {'success': False, 'message': str(e)}
            
            # 同步指标
            try:
                logger.info("步骤 3/3: 同步指标...")
                metrics_result = self._sync_metrics()
                results['metrics'] = metrics_result
                if not metrics_result.get('success'):
                    results['success'] = False
                    results['errors'].append(f"指标同步失败: {metrics_result.get('message')}")
            except Exception as e:
                logger.error(f"指标同步异常: {e}", exc_info=True)
                results['success'] = False
                results['errors'].append(f"指标同步异常: {str(e)}")
                results['metrics'] = {'success': False, 'message': str(e)}
            
            # 计算耗时
            elapsed_time = time.time() - start_time
            results['end_time'] = datetime.now().isoformat()
            results['elapsed_seconds'] = round(elapsed_time, 2)
            
            # 记录结果
            self.last_sync_time = datetime.now()
            self.last_sync_status = results
            
            if results['success']:
                logger.info(f"✅ 数据同步完成！耗时 {elapsed_time:.2f} 秒")
            else:
                logger.error(f"❌ 数据同步部分失败，耗时 {elapsed_time:.2f} 秒")
                logger.error(f"错误: {results['errors']}")
            
            logger.info("=" * 60)
            return results
            
        finally:
            self.is_syncing = False
            self.sync_lock.release()
    
    def _sync_metadata(self) -> Dict[str, Any]:
        """同步元数据"""
        try:
            if not self.table_ids:
                return {
                    'success': False,
                    'message': '表ID列表为空',
                    'fields_loaded': 0,
                    'fields_indexed': 0
                }
            
            # 从API加载元数据
            logger.info(f"  从API加载元数据（表ID: {self.table_ids}）...")
            loader = MetadataLoader(jwt=config.METADATA_API_JWT)
            fields = loader.load_from_api(self.table_ids)
            
            if not fields:
                return {
                    'success': False,
                    'message': 'API返回的元数据为空',
                    'fields_loaded': 0,
                    'fields_indexed': 0
                }
            
            logger.info(f"  ✅ 从API加载了 {len(fields)} 个字段")
            
            # 更新ES索引
            logger.info(f"  更新Elasticsearch索引...")
            if not self.hybrid_searcher.es_engine:
                return {
                    'success': False,
                    'message': 'Elasticsearch引擎不可用',
                    'fields_loaded': len(fields),
                    'fields_indexed': 0
                }
            
            # 强制重建索引（删除旧索引，创建新索引）
            logger.info("  删除旧索引并重建...")
            self.hybrid_searcher.es_engine.create_index(force=True)
            
            # 批量索引
            index_result = self.hybrid_searcher.es_engine.bulk_index_fields(fields)
            indexed_count = index_result.get('success', 0)
            
            logger.info(f"  ✅ 成功索引 {indexed_count} 个字段")
            
            # 重新初始化AC自动机和相似度匹配器
            logger.info(f"  重新初始化搜索引擎...")
            if self.hybrid_searcher.ac_matcher:
                self.hybrid_searcher.ac_matcher.initialize(fields)
                logger.info("  ✅ AC自动机已更新")
            
            if self.hybrid_searcher.similarity_matcher:
                self.hybrid_searcher.similarity_matcher.initialize(fields)
                logger.info("  ✅ 相似度匹配器已更新")
            
            # 更新字段数据缓存
            self.hybrid_searcher.fields_data = fields
            
            return {
                'success': True,
                'message': f'成功同步 {indexed_count} 个字段',
                'fields_loaded': len(fields),
                'fields_indexed': indexed_count,
                'fields': fields  # 返回字段列表供维度值同步使用
            }
            
        except Exception as e:
            logger.error(f"元数据同步失败: {e}", exc_info=True)
            return {
                'success': False,
                'message': str(e),
                'fields_loaded': 0,
                'fields_indexed': 0
            }
    
    def _sync_metrics(self) -> Dict[str, Any]:
        """同步指标"""
        try:
            # 从API加载指标
            logger.info(f"  从API加载指标...")
            loader = MetricLoader(jwt=config.METADATA_API_JWT)
            metrics = loader.load_from_api(max_workers=10)
            
            if not metrics:
                return {
                    'success': False,
                    'message': 'API返回的指标为空',
                    'metrics_loaded': 0,
                    'metrics_indexed': 0
                }
            
            logger.info(f"  ✅ 从API加载了 {len(metrics)} 个指标")
            
            # 更新ES索引
            logger.info(f"  更新Elasticsearch索引...")
            if not self.hybrid_searcher.es_engine:
                return {
                    'success': False,
                    'message': 'Elasticsearch引擎不可用',
                    'metrics_loaded': len(metrics),
                    'metrics_indexed': 0
                }
            
            # 强制重建指标索引（删除旧索引，创建新索引）
            logger.info("  删除旧指标索引并重建...")
            self.hybrid_searcher.es_engine.create_metric_index(force=True)
            
            # 批量索引
            success = self.hybrid_searcher.es_engine.bulk_index_metrics(metrics)
            
            if success:
                logger.info(f"  ✅ 成功索引 {len(metrics)} 个指标")
                return {
                    'success': True,
                    'message': f'成功同步 {len(metrics)} 个指标',
                    'metrics_loaded': len(metrics),
                    'metrics_indexed': len(metrics)
                }
            else:
                return {
                    'success': False,
                    'message': '指标索引失败',
                    'metrics_loaded': len(metrics),
                    'metrics_indexed': 0
                }
            
        except Exception as e:
            logger.error(f"指标同步失败: {e}", exc_info=True)
            return {
                'success': False,
                'message': str(e),
                'metrics_loaded': 0,
                'metrics_indexed': 0
            }
    
    def _sync_dimension_values(self, fields: List[MetadataField]) -> Dict[str, Any]:
        """
        同步维度值
        
        Args:
            fields: 元数据字段列表（从 _sync_metadata 传入）
        """
        try:
            from indexing.dimension_extractor import EnhancedDimensionExtractor
            
            # 过滤出维度字段
            dimension_fields = [f for f in fields if f.field_type == 'DIMENSION']
            
            if not dimension_fields:
                logger.info("  没有维度字段，跳过维度值提取")
                return {
                    'success': True,
                    'message': '没有维度字段',
                    'dimension_values_extracted': 0,
                    'dimension_values_indexed': 0
                }
            
            logger.info(f"  找到 {len(dimension_fields)} 个维度字段")
            
            # 更新ES索引
            if not self.hybrid_searcher.es_engine:
                return {
                    'success': False,
                    'message': 'Elasticsearch引擎不可用',
                    'dimension_values_extracted': 0,
                    'dimension_values_indexed': 0
                }
            
            # 强制重建维度值索引
            logger.info("  删除旧维度值索引并重建...")
            dimension_index_created = self.hybrid_searcher.es_engine.create_dimension_values_index(force=True)
            
            if not dimension_index_created:
                return {
                    'success': False,
                    'message': '维度值索引创建失败',
                    'dimension_values_extracted': 0,
                    'dimension_values_indexed': 0
                }
            
            # 提取维度值
            logger.info("  从数据库提取维度值...")
            extractor = EnhancedDimensionExtractor()
            dimension_values = extractor.extract_all_dimension_values(fields)
            extractor.close_connections()
            
            if not dimension_values:
                logger.info("  未提取到维度值")
                return {
                    'success': True,
                    'message': '未提取到维度值',
                    'dimension_values_extracted': 0,
                    'dimension_values_indexed': 0
                }
            
            logger.info(f"  ✅ 从数据库提取了 {len(dimension_values)} 个维度值")
            
            # 批量索引维度值
            index_result = self.hybrid_searcher.es_engine.bulk_index_dimension_values(dimension_values)
            indexed_count = index_result.get('success', 0)
            
            logger.info(f"  ✅ 成功索引 {indexed_count} 个维度值")
            
            return {
                'success': True,
                'message': f'成功同步 {indexed_count} 个维度值',
                'dimension_values_extracted': len(dimension_values),
                'dimension_values_indexed': indexed_count
            }
            
        except Exception as e:
            logger.error(f"维度值同步失败: {e}", exc_info=True)
            return {
                'success': False,
                'message': str(e),
                'dimension_values_extracted': 0,
                'dimension_values_indexed': 0
            }
    
    def get_status(self) -> Dict[str, Any]:
        """获取同步状态"""
        status = {
            'enabled': config.API_SYNC_ENABLED,
            'sync_mode': self.sync_mode,
            'table_ids': self.table_ids,
            'is_syncing': self.is_syncing,
            'last_sync_time': self.last_sync_time.isoformat() if self.last_sync_time else None,
            'last_sync_status': self.last_sync_status,
            'scheduler_running': self.scheduler.running if self.scheduler else False
        }
        
        # 根据模式添加不同的配置信息
        if self.sync_mode == 'interval':
            status['interval_hours'] = self.sync_interval_hours
        else:
            status['cron_time'] = self.sync_cron_time
        
        return status

