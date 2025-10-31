# 删除索引API使用说明

## 📋 概述

新增了一个用于删除Elasticsearch索引的管理接口，支持选择性删除或全部删除系统中的索引。

**端点**: `DELETE /api/search/index/delete`

**⚠️ 警告**: 这是一个危险操作，会永久删除索引数据！使用前请确认。

## 🎯 功能特点

1. **选择性删除** - 可以选择删除哪些索引
2. **安全确认机制** - 必须设置`confirm=true`才能执行
3. **详细的操作反馈** - 返回每个索引的删除状态
4. **自动重置** - 删除后自动重置搜索器初始化状态

## 📊 支持的索引类型

系统包含三种索引：

1. **元数据字段索引** (fields_index) - 存储元数据字段信息
2. **维度值索引** (dimension_values_index) - 存储维度字段的具体值
3. **指标索引** (metrics_index) - 存储业务指标信息

## 🔧 API参数

| 参数 | 类型 | 必需 | 默认值 | 说明 |
|------|------|------|--------|------|
| delete_fields_index | bool | ❌ | true | 是否删除元数据字段索引 |
| delete_dimension_values_index | bool | ❌ | true | 是否删除维度值索引 |
| delete_metrics_index | bool | ❌ | true | 是否删除指标索引 |
| confirm | bool | ✅ | false | 必须设置为true才能执行删除 |

## 📝 使用示例

### 示例1: 删除所有索引

```bash
# 使用curl
curl -X DELETE "http://localhost:8083/api/search/index/delete?confirm=true"

# 或使用详细参数
curl -X DELETE "http://localhost:8083/api/search/index/delete?delete_fields_index=true&delete_dimension_values_index=true&delete_metrics_index=true&confirm=true"
```

**响应示例**:
```json
{
  "success": true,
  "message": "成功删除 3 个索引",
  "results": {
    "deleted_indices": [
      {
        "name": "kman_metadata_fields",
        "type": "元数据字段索引"
      },
      {
        "name": "kman_metadata_dimension_values",
        "type": "维度值索引"
      },
      {
        "name": "kman_metadata_metrics",
        "type": "指标索引"
      }
    ],
    "failed_indices": [],
    "skipped_indices": []
  },
  "summary": {
    "total_requested": 3,
    "deleted": 3,
    "failed": 0,
    "skipped": 0
  }
}
```

### 示例2: 只删除指标索引

```bash
curl -X DELETE "http://localhost:8083/api/search/index/delete?delete_fields_index=false&delete_dimension_values_index=false&delete_metrics_index=true&confirm=true"
```

**响应示例**:
```json
{
  "success": true,
  "message": "成功删除 1 个索引",
  "results": {
    "deleted_indices": [
      {
        "name": "kman_metadata_metrics",
        "type": "指标索引"
      }
    ],
    "failed_indices": [],
    "skipped_indices": []
  },
  "summary": {
    "total_requested": 1,
    "deleted": 1,
    "failed": 0,
    "skipped": 0
  }
}
```

### 示例3: 只删除元数据字段索引和维度值索引

```bash
curl -X DELETE "http://localhost:8083/api/search/index/delete?delete_fields_index=true&delete_dimension_values_index=true&delete_metrics_index=false&confirm=true"
```

### 示例4: 没有确认参数（会失败）

```bash
curl -X DELETE "http://localhost:8083/api/search/index/delete"
```

**错误响应**:
```json
{
  "detail": "必须设置 confirm=true 参数才能执行删除操作"
}
```

## 🌐 在Swagger UI中使用

1. 访问API文档：http://localhost:8083/docs
2. 找到 `DELETE /api/search/index/delete` 端点
3. 点击 "Try it out"
4. 设置参数：
   - 选择要删除的索引类型
   - **重要**: 勾选 `confirm` 为 `true`
5. 点击 "Execute"
6. 查看响应结果

## 📤 响应格式

### 成功响应

```json
{
  "success": true/false,
  "message": "操作结果消息",
  "results": {
    "deleted_indices": [
      {
        "name": "索引名称",
        "type": "索引类型"
      }
    ],
    "failed_indices": [
      {
        "name": "索引名称",
        "type": "索引类型",
        "error": "错误信息"
      }
    ],
    "skipped_indices": [
      {
        "name": "索引名称",
        "type": "索引类型",
        "reason": "跳过原因"
      }
    ]
  },
  "summary": {
    "total_requested": 3,
    "deleted": 2,
    "failed": 0,
    "skipped": 1
  }
}
```

### 字段说明

- **success**: 操作是否完全成功（没有失败的删除）
- **message**: 操作结果的简要说明
- **results.deleted_indices**: 成功删除的索引列表
- **results.failed_indices**: 删除失败的索引列表（包含错误信息）
- **results.skipped_indices**: 跳过的索引列表（通常是因为索引不存在）
- **summary**: 操作统计摘要

## ⚠️ 注意事项

### 1. 数据不可恢复
删除索引后，所有索引数据将永久丢失，无法恢复。

### 2. 需要重建索引
删除索引后，如果需要继续使用系统，必须重建索引：

```bash
# 重建所有索引
curl -X POST "http://localhost:8083/api/search/index/create" \
  -H "Content-Type: application/json" \
  -d '{
    "force_recreate": true,
    "auto_load_data": true
  }'
```

### 3. 自动重置
删除索引后，系统会自动重置搜索器的初始化状态，下次搜索请求时会自动重新初始化。

### 4. 索引名称
当前索引名称（基于ES_INDEX_PREFIX=kman）：
- 元数据字段索引: `kman_metadata_fields`
- 维度值索引: `kman_metadata_dimension_values`
- 指标索引: `kman_metadata_metrics`

如果修改了ES_INDEX_PREFIX配置，索引名称会相应改变。

### 5. 权限
此接口应该在生产环境中受到适当的访问控制保护。

## 🔄 常见使用场景

### 场景1: 完全重置系统
```bash
# 1. 删除所有索引
curl -X DELETE "http://localhost:8083/api/search/index/delete?confirm=true"

# 2. 重新创建索引并加载数据
curl -X POST "http://localhost:8083/api/search/index/create" \
  -H "Content-Type: application/json" \
  -d '{"force_recreate": true, "auto_load_data": true}'
```

### 场景2: 更新指标数据
```bash
# 1. 只删除指标索引
curl -X DELETE "http://localhost:8083/api/search/index/delete?delete_fields_index=false&delete_dimension_values_index=false&delete_metrics_index=true&confirm=true"

# 2. 重新创建索引（会自动重建指标索引）
curl -X POST "http://localhost:8083/api/search/index/create" \
  -H "Content-Type: application/json" \
  -d '{"force_recreate": false, "auto_load_data": true}'
```

### 场景3: 测试环境清理
```bash
# 删除所有索引进行清理
curl -X DELETE "http://localhost:8083/api/search/index/delete?confirm=true"
```

## 🛡️ 安全建议

1. **生产环境**: 建议为此接口添加身份验证和授权
2. **审计日志**: 所有删除操作都会记录在应用日志中
3. **备份策略**: 在删除索引前，考虑备份重要数据
4. **分阶段删除**: 在不确定的情况下，先删除单个索引测试

## 📞 故障排除

### 问题1: 删除失败
**原因**: Elasticsearch连接问题或索引被锁定
**解决**: 检查ES连接状态，查看日志获取详细错误信息

### 问题2: 部分索引删除失败
**原因**: 某些索引可能不存在或权限不足
**解决**: 查看响应中的`failed_indices`获取具体错误

### 问题3: 删除后系统无法使用
**原因**: 索引已删除但未重建
**解决**: 调用`/api/search/index/create`接口重建索引

## 🔗 相关接口

- `POST /api/search/index/create` - 创建/重建索引
- `GET /api/search/stats` - 查看系统统计信息
- `GET /api/search/health` - 健康检查

---

**版本**: v1.0
**添加时间**: 2025年10月22日
**接口类型**: 管理接口

