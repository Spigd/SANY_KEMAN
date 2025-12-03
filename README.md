# 元数据搜索系统 - API数据源增强版

🚀 **支持API数据源、自动同步调度、混合检索和维度值索引的元数据搜索系统**

## ✨ 核心特性

### 📡 API数据源支持

- **智能数据加载**: 自动从API或Excel加载元数据和指标数据
- **JWT认证**: 支持JWT Token认证，安全访问API
- **自动同步调度**: 定时从API同步数据到Elasticsearch（可配置间隔）
- **手动同步接口**: 提供手动触发同步的API接口
- **多表支持**: 支持从多个表ID批量加载元数据
- **指标筛选**: 支持通过指标ID列表筛选需要加载的指标

### 🔍 混合检索技术

- **Elasticsearch全文搜索**: 强大的分词和相关性评分
- **AC自动机精确匹配**: 高速字符串匹配，支持多模式匹配
- **相似度匹配**: 基于文本相似度的语义搜索
- **维度值搜索**: 对维度字段的具体值进行精确搜索
- **指标搜索**: 支持搜索指标名称、别名、相关实体等
- **智能结果合并**: 多引擎结果加权合并，提供最优搜索体验

### 🎯 维度值索引

- **字段类型识别**: 自动区分 `DIMENSION`、`METRIC`、`ATTRIBUTE` 字段类型
- **维度值提取**: 从源数据库(MySQL/PostgreSQL)中提取维度列的所有可能值
- **维度值索引**: 为维度值建立独立的Elasticsearch索引，支持快速检索
- **多数据源支持**: 支持从多个数据库源并行提取维度值
- **自动同步**: 在元数据同步后自动提取并索引维度值

### 🎛️ 智能分词控制

- **可控分词**: 支持开启/关闭分词，适应不同搜索场景
- **多分词器支持**: IK中文分词器(ik_max_word/ik_smart)和标准分词器
- **场景适配**:
  - 启用分词：适合复杂查询和长文本搜索
  - 禁用分词：适合专业术语和精确匹配

### ⚡ 一键部署

- **自动索引创建**: 一键创建字段索引、指标索引和维度值索引
- **智能数据加载**: 根据配置自动选择API或Excel数据源
- **维度值自动提取**: 自动从数据库提取并索引维度值
- **多引擎初始化**: 同时初始化ES、AC自动机、相似度匹配器
- **索引状态检查**: 启动时自动检查三个索引状态，智能初始化

## 📊 系统架构

```
元数据搜索系统
├── 🔍 混合搜索层
│   ├── ElasticsearchEngine    # ES全文搜索 + 维度值搜索 + 指标搜索
│   ├── ACMatcher             # AC自动机匹配
│   ├── SimilarityMatcher     # 相似度匹配
│   └── HybridSearcher        # 混合搜索控制器
├── 📊 数据层
│   ├── MetadataLoader        # 智能数据加载器（API/Excel）
│   ├── MetricLoader          # 指标数据加载器（API/Excel）
│   ├── MetadataAPIClient     # 元数据API客户端
│   ├── MetricAPIClient       # 指标API客户端
│   ├── DatabaseManager       # 数据库连接管理器
│   ├── DimensionExtractor    # 维度值提取器
│   └── DataSyncScheduler     # 数据同步调度器（新增）
├── 🌐 API层
│   ├── SearchAPI             # 统一搜索接口
│   ├── SyncAPI               # 数据同步接口（新增）
│   ├── DimensionAPI          # 维度值搜索接口
│   └── DatabaseAPI           # 数据库管理接口
└── 💾 存储层
    ├── Elasticsearch         # 字段索引存储
    ├── Elasticsearch         # 指标索引存储（新增）
    └── Elasticsearch         # 维度值索引存储
```

## 🚀 快速开始

### 1. 环境准备

```bash
# 安装Python依赖
pip install -r requirements.txt

# 启动Elasticsearch (Docker方式)
docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -p 9300:9300 \
  -e "discovery.type=single-node" \
  -e "ES_JAVA_OPTS=-Xms512m -Xmx512m" \
  elasticsearch:8.11.0

# 安装IK中文分词器（可选，推荐）
docker exec -it elasticsearch elasticsearch-plugin install https://github.com/medcl/elasticsearch-analysis-ik/releases/download/v8.11.0/elasticsearch-analysis-ik-8.11.0.zip
docker restart elasticsearch
```

### 2. 配置系统

#### 方式1: 环境变量配置

```bash
# 设置环境变量
export ES_HOST=10.66.0.160
export ES_PORT=9200
export ES_INDEX_PREFIX=kman

# API数据源配置
export METADATA_API_BASE_URL=https://metric-asset-api-internal.rootcloudapp.com
export METADATA_API_JWT=your_jwt_token_here
export API_SYNC_ENABLED=true
export API_SYNC_INTERVAL=2
export API_TABLE_IDS=268,269,270
export API_METRIC_IDS=171,172,357
```

#### 方式2: Docker环境变量

```dockerfile
# 在Dockerfile或docker-compose.yml中配置
ENV METADATA_API_BASE_URL=https://metric-asset-api-internal.rootcloudapp.com
ENV METADATA_API_JWT=your_jwt_token_here
ENV API_SYNC_ENABLED=true
ENV API_SYNC_INTERVAL=2
ENV API_TABLE_IDS=268,269,270
ENV API_METRIC_IDS=171,172,357
```

#### 方式3: .env文件配置

创建 `.env` 文件：

```env
# Elasticsearch配置
ES_HOST=10.66.0.160
ES_PORT=9200
ES_INDEX_PREFIX=kman

# API配置
API_HOST=0.0.0.0
API_PORT=8082

# 元数据和指标API配置
METADATA_API_BASE_URL=https://metric-asset-api-internal.rootcloudapp.com
METADATA_API_TIMEOUT=30
METADATA_API_JWT=your_jwt_token_here

# API数据同步配置
API_SYNC_ENABLED=true
API_SYNC_INTERVAL=2
API_TABLE_IDS=268,269,270
API_METRIC_IDS=171,172,357

# 混合搜索权重
ES_WEIGHT=1.0
AC_WEIGHT=0.9
SIM_WEIGHT=0.8

# 分词器配置
DEFAULT_TOKENIZER=ik_max_word
DEFAULT_SEARCH_ANALYZER=ik_smart

# 数据库连接配置（用于维度值提取）
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_username
DB_PASSWORD=your_password
DB_DATABASE=your_database

# 维度值索引配置
DIMENSION_VALUE_INDEXING_ENABLED=true
MAX_VALUES_PER_COLUMN=1000
AUTO_EXTRACT_DIMENSIONS=true
```

### 3. 数据源模式

系统支持两种数据源模式，通过 `API_SYNC_ENABLED` 环境变量控制：

#### API模式（推荐）

```env
API_SYNC_ENABLED=true
```

- 从API接口加载元数据和指标数据
- 支持自动定时同步
- 支持手动触发同步
- 需要配置 `METADATA_API_BASE_URL` 和 `METADATA_API_JWT`

#### Excel模式（兼容模式）

```env
API_SYNC_ENABLED=false
```

- 从Excel文件加载元数据和指标数据
- 需要提供 `客满-元数据表.xlsx` 和 `metric_latest.xlsx` 文件

### 4. 启动系统

```bash
python run.py
```

系统将在 http://localhost:8082 启动

### 5. 初始化索引

#### 自动初始化

系统启动时会自动检查索引状态：
- 如果三个索引（字段、指标、维度值）都存在且有数据，跳过初始化
- 如果索引缺失，自动创建并加载数据

#### 手动初始化

访问 API 文档: http://localhost:8082/docs

```bash
# 创建索引并加载数据
curl -X POST "http://localhost:8082/api/search/index/create" \
  -H "Content-Type: application/json" \
  -d '{
    "force_recreate": true
  }'
```

## 🔍 API 使用指南

### 基础字段搜索

```bash
# 混合搜索（推荐）
curl "http://localhost:8082/api/search/fields?q=客户编码&search_method=hybrid"

# 分词搜索
curl "http://localhost:8082/api/search/fields?q=客户编码&use_tokenization=true&tokenizer_type=ik_max_word"

# 精确匹配（不分词）
curl "http://localhost:8082/api/search/fields?q=客户编码&use_tokenization=false"
```

### 指标搜索

```bash
# 搜索指标（GET）
curl "http://localhost:8082/api/search/metrics?q=有效作业率"

# 搜索指标（POST）
curl -X POST "http://localhost:8082/api/search/metrics" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "有效作业率",
    "size": 10,
    "use_tokenization": true
  }'
```

### 维度值搜索

```bash
# 搜索维度值
curl "http://localhost:8082/api/search/dimension-values?q=已完成"

# 限制表名搜索
curl "http://localhost:8082/api/search/dimension-values?q=VIP&table_name=customer_info"

# 限制列名搜索
curl "http://localhost:8082/api/search/dimension-values?q=北京&column_name=region"
```

### 🆕 数据同步API

#### 手动同步元数据

```bash
# 同步元数据（使用环境变量配置的表ID）
curl -X POST "http://localhost:8082/api/search/sync/metadata"

# 指定表ID同步
curl -X POST "http://localhost:8082/api/search/sync/metadata?table_ids=268&table_ids=269"

# 使用自定义JWT
curl -X POST "http://localhost:8082/api/search/sync/metadata?jwt=your_custom_jwt"
```

#### 手动同步指标

```bash
# 同步所有指标
curl -X POST "http://localhost:8082/api/search/sync/metrics"

# 同步指定指标ID
curl -X POST "http://localhost:8082/api/search/sync/metrics?ids=171,172,357"

# 强制重建索引
curl -X POST "http://localhost:8082/api/search/sync/metrics?force=true"
```

#### 手动同步维度值

```bash
# 同步维度值
curl -X POST "http://localhost:8082/api/search/sync/dimension-values"

# 强制重建维度值索引
curl -X POST "http://localhost:8082/api/search/sync/dimension-values?force=true"
```

#### 查看同步状态

```bash
# 获取同步调度器状态
curl "http://localhost:8082/api/search/sync/status"
```

响应示例：

```json
{
  "enabled": true,
  "interval_hours": 2,
  "table_ids": [268, 269, 270],
  "is_syncing": false,
  "last_sync_time": "2025-12-03T10:00:00",
  "last_sync_status": {
    "success": true,
    "metadata": {...},
    "dimension_values": {...},
    "metrics": {...}
  },
  "scheduler_running": true
}
```

### 高级搜索

```bash
# POST方式复杂查询
curl -X POST "http://localhost:8082/api/search/fields" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "客户信息",
    "search_method": "hybrid",
    "use_tokenization": true,
    "tokenizer_type": "ik_smart",
    "table_name": ["dwd_customer_info"],
    "enabled_only": true,
    "size": 20
  }'
```

### 系统管理

```bash
# 获取系统状态
curl "http://localhost:8082/api/search/stats"

# 健康检查
curl "http://localhost:8082/api/search/health"

# 删除索引（谨慎操作）
curl -X DELETE "http://localhost:8082/api/search/index/delete?confirm=true"
```

## 📝 搜索方法对比

| 搜索方法                      | 优势                       | 适用场景           | 性能       |
| ----------------------------- | -------------------------- | ------------------ | ---------- |
| **hybrid**              | 综合多种算法优势           | 通用搜索，推荐使用 | ⭐⭐⭐⭐⭐ |
| **elasticsearch**       | 强大的全文搜索和相关性评分 | 复杂文本查询       | ⭐⭐⭐⭐   |
| **dimension_values** 🆕 | 精确的维度值匹配           | 查找具体的维度值   | ⭐⭐⭐⭐⭐ |
| **metrics** 🆕          | 指标名称和别名搜索         | 查找指标定义       | ⭐⭐⭐⭐   |
| **ac_matcher**          | 极快的精确匹配             | 已知术语查找       | ⭐⭐⭐⭐⭐ |
| **similarity**          | 语义相似度匹配             | 模糊查询           | ⭐⭐⭐     |

## 🎯 维度值索引使用场景

### 什么是维度值索引？

维度值索引是系统的核心功能，它会：

1. **识别维度字段**: 自动识别或手动标记为 `DIMENSION` 类型的字段
2. **提取所有可能值**: 从源数据库中查询该字段的所有 DISTINCT 值
3. **建立独立索引**: 为这些值创建专门的Elasticsearch索引
4. **支持精确搜索**: 用户可以直接搜索这些具体的维度值

### 典型使用场景

**场景1: 订单状态查询**

- 字段：`order_status` (维度字段)
- 可能值：`待付款`、`已付款`、`配送中`、`已完成`、`已取消`
- 搜索：用户输入"已完成"可以直接找到相关的状态字段

**场景2: 地区信息查询**

- 字段：`region` (维度字段)
- 可能值：`北京`、`上海`、`广州`、`深圳`...
- 搜索：用户输入"北京"可以找到所有包含北京地区的相关字段

**场景3: 用户等级查询**

- 字段：`user_level` (维度字段)
- 可能值：`普通用户`、`VIP用户`、`钻石用户`
- 搜索：用户输入"VIP"可以精确匹配到用户等级相关字段

## 📊 搜索响应格式

### 字段搜索响应

```json
{
  "query": "客户编码",
  "total": 15,
  "took": 45,
  "search_methods": ["elasticsearch", "ac_matcher", "similarity"],
  "tokenization_used": true,
  "tokenizer_type": "ik_max_word",
  "results": [
    {
      "field": {
        "table_name": "dwd_customer_info",
        "column_name": "customer_code",
        "chinese_name": "客户编码",
        "field_type": "DIMENSION",
        "alias": ["客户唯一ID", "customer_code"],
        "description": "客户唯一标识编码",
        "is_effect": true
      },
      "score": 8.234567,
      "matched_text": "chinese_name: 客户编码",
      "search_method": "elasticsearch",
      "highlight": {
        "chinese_name": ["<em>客户编码</em>"]
      }
    }
  ]
}
```

### 指标搜索响应

```json
{
  "query": "有效作业率",
  "total": 1,
  "took": 12,
  "search_methods": ["elasticsearch"],
  "results": [
    {
      "metric": {
        "metric_id": 357,
        "metric_name": "有效作业率",
        "metric_alias": ["有效作业率1", "有效作业率2"],
        "related_entities": ["entity1", "entity2"],
        "business_definition": "{m12}",
        "depends_on_tables": ["table1", "table2"],
        "depends_on_columns": ["table1", "table2"],
        "metric_sql": "实际作业时间占开机时间的比例"
      },
      "score": 9.876543,
      "matched_text": "metric_name: 有效作业率",
      "search_method": "elasticsearch"
    }
  ]
}
```

### 维度值搜索响应

```json
{
  "query": "已完成",
  "total": 3,
  "took": 12,
  "search_methods": ["dimension_values"],
  "results": [
    {
      "field": {
        "table_name": "order_info",
        "column_name": "order_status",
        "chinese_name": "订单状态",
        "field_type": "DIMENSION",
        "description": "维度值: 已完成"
      },
      "score": 9.876543,
      "matched_text": "维度值: 已完成",
      "search_method": "dimension_values",
      "extra_info": {
        "dimension_value": "已完成",
        "frequency": 15420,
        "value_hash": "abc123def456"
      }
    }
  ]
}
```

## 🛠️ 项目结构

```
KEMAN/
├── api/                    # API接口层
│   ├── __init__.py
│   ├── main.py            # FastAPI主应用
│   └── search_api.py      # 搜索API路由
├── core/                  # 核心模块
│   ├── __init__.py
│   ├── config.py          # 配置管理
│   ├── models.py          # 数据模型
│   └── database.py        # 数据库连接抽象层
├── search/                # 搜索引擎层
│   ├── __init__.py
│   ├── elasticsearch_engine.py  # ES搜索引擎
│   ├── ac_matcher.py           # AC自动机
│   ├── similarity_matcher.py   # 相似度匹配
│   └── hybrid_searcher.py      # 混合搜索器
├── indexing/              # 数据索引层
│   ├── __init__.py
│   ├── data_loader.py     # 智能数据加载器（API/Excel）
│   ├── dimension_extractor.py  # 维度值提取器
│   ├── scheduler.py       # 数据同步调度器（新增）
│   └── cal.py            # 综合分析模块
├── Dockerfile            # Docker构建文件
├── requirements.txt      # Python依赖
├── run.py               # 启动脚本
├── README.md            # 项目文档
└── 客满-元数据表.xlsx    # 元数据文件（Excel模式）
```

## ⚙️ 高级配置

### API数据源配置

```env
# API基础地址
METADATA_API_BASE_URL=https://metric-asset-api-internal.rootcloudapp.com

# API超时时间（秒）
METADATA_API_TIMEOUT=30

# JWT认证Token
METADATA_API_JWT=your_jwt_token_here

# 启用API同步模式
API_SYNC_ENABLED=true

# 自动同步间隔（小时）
API_SYNC_INTERVAL=2

# 要同步的表ID列表（逗号分隔）
API_TABLE_IDS=268,269,270

# 要同步的指标ID列表（逗号分隔，可选）
API_METRIC_IDS=171,172,357
```

### 维度值索引配置

```env
# 启用/禁用维度值索引
DIMENSION_VALUE_INDEXING_ENABLED=true

# 每列最大提取值数量
MAX_VALUES_PER_COLUMN=1000

# 批量处理大小
DIMENSION_BATCH_SIZE=100

# 是否在索引创建时自动提取维度值
AUTO_EXTRACT_DIMENSIONS=true
```

### 多数据库配置

支持从多个数据库源提取维度值：

```env
# 方式1: 环境变量 (单个数据库)
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_USER=username
DB_PASSWORD=password
DB_DATABASE=database

# 方式2: JSON配置 (多个数据库)
DATABASE_CONFIGS_JSON={"source1":{"type":"mysql","host":"host1","port":3306,"user":"user1","password":"pass1","database":"db1"},"source2":{"type":"postgresql","host":"host2","port":5432,"user":"user2","password":"pass2","database":"db2"}}
```

### 混合搜索权重调优

```env
# 调整各搜索引擎权重
ES_WEIGHT=1.0        # Elasticsearch权重
AC_WEIGHT=0.9        # AC自动机权重  
SIM_WEIGHT=0.8       # 相似度匹配权重
```

### 分词器选择

```env
# IK最大词长分词器（推荐）
DEFAULT_TOKENIZER=ik_max_word

# IK智能分词器（更精准）
DEFAULT_TOKENIZER=ik_smart

# 标准分词器（英文）
DEFAULT_TOKENIZER=standard
```

## 🚨 故障排除

### 常见问题

1. **Elasticsearch连接失败**

   ```bash
   # 检查ES服务状态
   curl http://localhost:9200/_cluster/health
   ```

2. **API认证失败（401错误）**

   ```bash
   # 检查JWT Token是否有效
   # 确保 METADATA_API_JWT 环境变量已正确设置
   ```

3. **数据库连接失败**

   ```bash
   # 测试数据库连接
   curl http://localhost:8082/api/search/database/test
   ```

4. **维度值提取失败**

   ```bash
   # 手动触发维度值提取
   curl -X POST "http://localhost:8082/api/search/dimension/extract?force_recreate=false"
   ```

5. **IK分词器不可用**

   ```bash
   # 安装IK分词器
   docker exec -it elasticsearch elasticsearch-plugin install https://github.com/medcl/elasticsearch-analysis-ik/releases/download/v8.11.0/elasticsearch-analysis-ik-8.11.0.zip
   docker restart elasticsearch
   ```

### 性能优化

1. **Elasticsearch优化**

   ```bash
   # 增加ES内存
   docker run -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" elasticsearch:8.11.0
   ```

2. **维度值索引优化**

   - 调整 `MAX_VALUES_PER_COLUMN` 限制每列提取的值数量
   - 使用 `DIMENSION_BATCH_SIZE` 控制批量处理大小
   - 合理配置数据库连接池

3. **搜索性能调优**

   - 调整搜索引擎权重
   - 限制搜索结果数量
   - 使用表名过滤减少搜索范围

4. **同步性能优化**

   - 调整 `API_SYNC_INTERVAL` 控制同步频率
   - 使用 `API_METRIC_IDS` 限制指标同步范围
   - 合理设置 `METADATA_API_TIMEOUT` 避免超时

## 📈 版本更新

### 最新版本特性

- ✅ **API数据源支持**: 支持从API接口加载元数据和指标数据
- ✅ **JWT认证**: 支持JWT Token认证访问API
- ✅ **自动同步调度**: 定时从API同步数据到Elasticsearch
- ✅ **手动同步接口**: 提供手动触发同步的API接口
- ✅ **指标搜索**: 支持搜索指标名称、别名、相关实体
- ✅ **三个索引管理**: 字段索引、指标索引、维度值索引
- ✅ **智能初始化**: 启动时自动检查索引状态，智能初始化
- ✅ **字段映射优化**: 使用 `chinese_name` 和 `alias` 替代旧字段名
- ✅ **字段类型标准化**: 统一使用大写（DIMENSION/METRIC/ATTRIBUTE）

### 数据源切换

系统支持在API模式和Excel模式之间切换：

**切换到API模式**:
```env
API_SYNC_ENABLED=true
METADATA_API_BASE_URL=https://your-api-url.com
METADATA_API_JWT=your_jwt_token
API_TABLE_IDS=268,269,270
API_METRIC_IDS=171,172,357
```

**切换到Excel模式**:
```env
API_SYNC_ENABLED=false
# 确保存在 Excel 文件：客满-元数据表.xlsx 和 metric_latest.xlsx
```

### 同步流程说明

#### 自动同步（调度器）

1. **元数据同步**: 从API加载元数据 → 强制重建字段索引 → 批量索引
2. **维度值同步**: 从数据库提取维度值 → 强制重建维度值索引 → 批量索引
3. **指标同步**: 从API加载指标 → 强制重建指标索引 → 批量索引

#### 手动同步

- `/sync/metadata`: 手动同步元数据
- `/sync/metrics`: 手动同步指标
- `/sync/dimension-values`: 手动同步维度值
- `/sync/status`: 查看同步状态

## 🤝 贡献指南

欢迎提交Issue和Pull Request来改进这个项目！

### 开发环境设置

```bash
git clone <repository>
cd KEMAN
pip install -r requirements.txt
python run.py
```

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

---

**🎯 元数据搜索系统 - 让搜索更智能，让数据更易用！**
