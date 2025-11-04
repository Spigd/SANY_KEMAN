# 元数据搜索系统 V4 - 维度值索引增强版

🚀 **全新升级的元数据搜索系统，支持混合检索、智能分词和维度值索引**

## ✨ 核心特性

### 🔍 混合检索技术

- **Elasticsearch全文搜索**: 强大的分词和相关性评分
- **AC自动机精确匹配**: 高速字符串匹配，支持多模式匹配
- **相似度匹配**: 基于文本相似度的语义搜索
- **维度值搜索**: 🆕 对维度字段的具体值进行精确搜索
- **智能结果合并**: 多引擎结果加权合并，提供最优搜索体验

### 🎯 维度值索引 (新功能)

- **字段类型识别**: 自动区分 `dimension` 和 `metric` 字段类型
- **维度值提取**: 从源数据库(MySQL/PostgreSQL)中提取维度列的所有可能值
- **维度值索引**: 为维度值建立独立的Elasticsearch索引，支持快速检索
- **多数据源支持**: 支持从多个数据库源并行提取维度值
- **智能推断**: 对于缺少 `field_type` 字段的历史数据，自动推断字段类型

### 🎛️ 智能分词控制

- **可控分词**: 支持开启/关闭分词，适应不同搜索场景
- **多分词器支持**: IK中文分词器(ik_max_word/ik_smart)和标准分词器
- **场景适配**:
  - 启用分词：适合复杂查询和长文本搜索
  - 禁用分词：适合专业术语和精确匹配

### ⚡ 一键部署

- **自动索引创建**: 一键创建字段索引和维度值索引
- **数据自动加载**: 从Excel文件自动导入元数据
- **维度值自动提取**: 自动从数据库提取并索引维度值
- **多引擎初始化**: 同时初始化ES、AC自动机、相似度匹配器

### 🔄 批量查询 API (新功能)

- **批量处理**: 支持一次性处理多个问题
- **并行执行**: 使用多线程并行调用 Dify API
- **灵活配置**: 可自定义并发数和超时时间
- **错误容错**: 部分失败不影响其他问题处理
- **完整响应**: 返回每个问题的答案和完整 API 响应

## 📊 系统架构

```
元数据搜索系统 V4
├── 🔍 混合搜索层
│   ├── ElasticsearchEngine    # ES全文搜索 + 维度值搜索
│   ├── ACMatcher             # AC自动机匹配
│   ├── SimilarityMatcher     # 相似度匹配
│   └── HybridSearcher        # 混合搜索控制器
├── 📊 数据层
│   ├── MetadataLoader        # Excel数据加载器
│   ├── DatabaseManager       # 数据库连接管理器 (新增)
│   └── DimensionExtractor    # 维度值提取器 (新增)
├── 🌐 API层
│   ├── SearchAPI             # 统一搜索接口
│   ├── DimensionAPI          # 维度值搜索接口 (新增)
│   └── DatabaseAPI           # 数据库管理接口 (新增)
└── 💾 存储层
    ├── Elasticsearch         # 字段索引存储
    └── Elasticsearch         # 维度值索引存储 (新增)
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

复制并编辑配置文件：

```bash
cp config.env.example config.env
```

编辑 `config.env`：

```env
# Elasticsearch配置
ES_HOST=localhost
ES_PORT=9200
ES_INDEX_PREFIX=metadata_v4

# API配置
API_HOST=0.0.0.0
API_PORT=8082

# 数据文件配置
METADATA_EXCEL_PATH=客满-元数据表.xlsx

# 混合搜索权重
ES_WEIGHT=1.0
AC_WEIGHT=0.9
SIM_WEIGHT=0.8

# 分词器配置
DEFAULT_TOKENIZER=ik_max_word
DEFAULT_SEARCH_ANALYZER=ik_smart

# 数据库连接配置 (新增 - 用于维度值提取)
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_username
DB_PASSWORD=your_password
DB_DATABASE=your_database

# 维度值索引配置 (新增)
DIMENSION_VALUE_INDEXING_ENABLED=true
MAX_VALUES_PER_COLUMN=1000
AUTO_EXTRACT_DIMENSIONS=true
```

### 3. 元数据表格式

您的Excel元数据表现在支持以下字段（**新增字段类型列**）：

| 列名               | 必需 | 说明                        |
| ------------------ | ---- | --------------------------- |
| 所属表             | ✅   | 数据表名                    |
| 列名               | ✅   | 字段名                      |
| 显示名称           | ✅   | 中文显示名                  |
| **字段类型** | 🆕   | `dimension` 或 `metric` |
| 同义词/别名        | ❌   | JSON数组格式                |
| 字段描述           | ❌   | 字段说明                    |
| 数据类型           | ❌   | text/number/datetime        |
| 是否实体           | ❌   | 布尔值                      |
| 是否启用           | ❌   | 布尔值                      |

**字段类型说明：**

- `dimension`: 维度字段，系统会从数据库中提取其所有可能值并建立索引
- `metric`: 指标字段，仅对字段名建立索引，不提取具体值

### 4. 启动系统

```bash
python run.py
```

系统将在 http://localhost:8082 启动

### 5. 一键初始化

访问 API 文档: http://localhost:8082/docs

调用初始化接口：

```bash
curl -X POST "http://localhost:8082/api/search/index/create" \
  -H "Content-Type: application/json" \
  -d '{
    "force_recreate": true,
    "auto_load_data": true
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

### 🆕 维度值搜索

```bash
# 搜索维度值
curl "http://localhost:8082/api/search/dimension-values?q=已完成"

# 限制表名搜索
curl "http://localhost:8082/api/search/dimension-values?q=VIP&table_name=customer_info"

# 限制列名搜索
curl "http://localhost:8082/api/search/dimension-values?q=北京&column_name=region"
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
    "table_name": "dwd_customer_info",
    "entity_only": true,
    "size": 20
  }'

# 维度值POST搜索
curl -X POST "http://localhost:8082/api/search/dimension-values" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "已完成",
    "table_name": "order_status",
    "use_tokenization": false,
    "size": 10
  }'
```

### 🆕 批量查询 API

批量查询 API 允许您一次性处理多个问题，并行调用 Dify API，适用于批量数据处理场景。

```bash
# 批量查询问题
curl -X POST "http://localhost:8082/api/search/batch-query" \
  -H "Content-Type: application/json" \
  -d '{
    "questions": ["问题1", "问题2", "问题3"],
    "api_url": "https://ai-aidq.sany.com.cn/v1/chat-messages",
    "jwt": "app-xxx",
    "jwt_chat": "Bearer xxx",
    "max_workers": 5,
    "timeout": 400
  }'
```

**响应格式**:

```json
{
    "success": true,
    "results": [
        {
            "id": 1,
            "question": "问题1",
            "answer": "这是答案...",
            "response": {...},
            "status": "success",
            "error": null,
            "timestamp": "2024-01-01T12:00:00"
        }
    ],
    "total": 3,
    "success_count": 3,
    "error_count": 0,
    "took": 5234
}
```

**参数说明**:

- `questions`: 问题列表 (必填)
- `api_url`: Dify API URL (必填)
- `jwt`: JWT token (必填)
- `jwt_chat`: 聊天 JWT token (必填)
- `max_workers`: 并发线程数，范围 1-20 (默认: 5)
- `timeout`: 超时时间（秒），范围 10-600 (默认: 400)

详细文档请参考: [BATCH_QUERY_API.md](BATCH_QUERY_API.md)

### 🆕 数据库管理

```bash
# 测试数据库连接
curl "http://localhost:8082/api/search/database/test"

# 验证维度字段
curl "http://localhost:8082/api/search/dimension/validate"

# 手动提取维度值
curl -X POST "http://localhost:8082/api/search/dimension/extract?force_recreate=false"

# 获取维度配置
curl "http://localhost:8082/api/search/config/dimension"
```

### 系统管理

```bash
# 获取系统状态
curl "http://localhost:8082/api/search/stats"

# 健康检查
curl "http://localhost:8082/api/search/health"

# 验证数据文件
curl "http://localhost:8082/api/search/validate"
```

## 📝 搜索方法对比

| 搜索方法                      | 优势                       | 适用场景           | 性能       |
| ----------------------------- | -------------------------- | ------------------ | ---------- |
| **hybrid**              | 综合多种算法优势           | 通用搜索，推荐使用 | ⭐⭐⭐⭐⭐ |
| **elasticsearch**       | 强大的全文搜索和相关性评分 | 复杂文本查询       | ⭐⭐⭐⭐   |
| **dimension_values** 🆕 | 精确的维度值匹配           | 查找具体的维度值   | ⭐⭐⭐⭐⭐ |
| **ac_matcher**          | 极快的精确匹配             | 已知术语查找       | ⭐⭐⭐⭐⭐ |
| **similarity**          | 语义相似度匹配             | 模糊查询           | ⭐⭐⭐     |

## 🎯 维度值索引使用场景

### 什么是维度值索引？

维度值索引是V4版本的核心新功能，它会：

1. **识别维度字段**: 自动识别或手动标记为 `dimension` 类型的字段
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
        "display_name": "客户编码",
        "field_type": "dimension",
        "synonyms": ["客户唯一ID", "customer_code"],
        "description": "客户唯一标识编码",
        "is_entity": true,
        "is_enabled": true
      },
      "score": 8.234567,
      "matched_text": "display_name: 客户编码",
      "search_method": "elasticsearch",
      "highlight": {
        "display_name": ["<em>客户编码</em>"]
      }
    }
  ]
}
```

### 🆕 维度值搜索响应

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
        "display_name": "订单状态",
        "field_type": "dimension",
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
es_search_system_v4/
├── api/                    # API接口层
│   ├── __init__.py
│   ├── main.py            # FastAPI主应用
│   └── search_api.py      # 搜索API路由 (已扩展)
├── core/                  # 核心模块
│   ├── __init__.py
│   ├── config.py          # 配置管理 (已扩展)
│   ├── models.py          # 数据模型 (已扩展)
│   └── database.py        # 数据库连接抽象层 (新增)
├── search/                # 搜索引擎层
│   ├── __init__.py
│   ├── elasticsearch_engine.py  # ES搜索引擎 (已扩展)
│   ├── ac_matcher.py           # AC自动机
│   ├── similarity_matcher.py   # 相似度匹配
│   └── hybrid_searcher.py      # 混合搜索器 (已扩展)
├── indexing/              # 数据索引层
│   ├── __init__.py
│   ├── data_loader.py     # Excel数据加载器 (已扩展)
│   └── dimension_extractor.py  # 维度值提取器 (新增)
├── config.env.example     # 配置文件模板 (已扩展)
├── requirements.txt       # Python依赖 (已扩展)
├── run.py                # 启动脚本
├── README.md             # 项目文档 (已重写)
└── 客满-元数据表.xlsx    # 元数据文件
```

## ⚙️ 高级配置

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
2. **数据库连接失败**

   ```bash
   # 测试数据库连接
   curl http://localhost:8082/api/search/database/test
   ```
3. **维度值提取失败**

   ```bash
   # 验证维度字段
   curl http://localhost:8082/api/search/dimension/validate
   ```
4. **IK分词器不可用**

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

## 📈 版本更新

### V4.0.0 新特性

- ✅ **维度值索引**: 支持从数据库提取维度值并建立索引
- ✅ **多数据库支持**: 支持MySQL和PostgreSQL数据源
- ✅ **字段类型识别**: 自动区分dimension和metric字段
- ✅ **数据库连接管理**: 抽象化数据库连接层
- ✅ **维度值搜索API**: 专门的维度值搜索接口
- ✅ **配置增强**: 支持维度值索引相关配置
- ✅ **向后兼容**: 完全兼容V3版本功能

### 从V3升级到V4

V4版本完全向后兼容V3，升级步骤：

1. **更新代码**: 拉取V4版本代码
2. **安装依赖**: `pip install -r requirements.txt`
3. **更新配置**: 添加数据库连接配置（可选）
4. **重启系统**: `python run.py`
5. **测试功能**: 访问 `/docs` 查看新增API

**注意**: 如果不配置数据库连接，系统仍然可以正常工作，只是不会有维度值索引功能。

## 🤝 贡献指南

欢迎提交Issue和Pull Request来改进这个项目！

### 开发环境设置

```bash
git clone <repository>
cd es_search_system_v4
pip install -r requirements.txt
python run.py
```

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

---

**🎯 元数据搜索系统 V4 - 让搜索更智能，让维度值触手可及！**
