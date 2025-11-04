# 批量查询 API 使用说明

## 概述

批量查询 API 允许您通过 HTTP 请求批量处理多个问题，并行调用 Dify API，返回每个问题的答案和完整响应。

## API 端点

```
POST /api/search/batch-query
```

## 请求格式

### 请求头
```
Content-Type: application/json
```

### 请求体
```json
{
    "questions": ["问题1", "问题2", "问题3"],
    "api_url": "https://ai-aidq.sany.com.cn/v1/chat-messages",
    "jwt": "app-ggdyQj8AEs4JaPER1TwWZXIr",
    "jwt_chat": "Bearer eyJhbGciOiJIUzUxMiJ9...",
    "max_workers": 5,
    "timeout": 400
}
```

### 参数说明

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| questions | List[str] | 是 | - | 问题列表 |
| api_url | str | 是 | - | Dify API 完整 URL |
| jwt | str | 是 | - | JWT token |
| jwt_chat | str | 是 | - | 聊天 JWT token |
| max_workers | int | 否 | 5 | 并发线程数 (1-20) |
| timeout | int | 否 | 400 | 请求超时时间（秒，10-600） |

## 响应格式

### 成功响应
```json
{
    "success": true,
    "results": [
        {
            "id": 1,
            "question": "问题1",
            "answer": "这是答案1的内容...",
            "response": {
                "answer": "这是答案1的内容...",
                "conversation_id": "...",
                "message_id": "..."
            },
            "status": "success",
            "error": null,
            "timestamp": "2024-01-01T12:00:00.123456"
        },
        {
            "id": 2,
            "question": "问题2",
            "answer": "[错误: 无响应数据]",
            "response": null,
            "status": "error",
            "error": "Connection timeout",
            "timestamp": "2024-01-01T12:00:05.123456"
        }
    ],
    "total": 2,
    "success_count": 1,
    "error_count": 1,
    "took": 5234
}
```

### 响应字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| success | bool | 整体是否全部成功（所有问题都成功才为 true） |
| results | List[BatchQueryResult] | 查询结果列表 |
| total | int | 总问题数 |
| success_count | int | 成功处理的问题数 |
| error_count | int | 失败的问题数 |
| took | int | 总耗时（毫秒） |

### BatchQueryResult 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| id | int | 问题ID（从1开始） |
| question | str | 问题文本 |
| answer | str \| null | 提取的答案（仅 status=success 时有效） |
| response | dict \| null | 完整的 API 响应 |
| status | str | 状态：success 或 error |
| error | str \| null | 错误信息（仅 status=error 时有值） |
| timestamp | str | 处理时间（ISO 格式） |

## 使用示例

### Python 示例

```python
import requests

url = "http://localhost:8000/api/search/batch-query"

payload = {
    "questions": [
        "各大区的拜访趋势数据如何？",
        "6月整体的满意度趋势如何",
        "集团各大区满意度横向比较"
    ],
    "api_url": "https://ai-aidq.sany.com.cn/v1/chat-messages",
    "jwt": "app-ggdyQj8AEs4JaPER1TwWZXIr",
    "jwt_chat": "Bearer eyJhbGciOiJIUzUxMiJ9...",
    "max_workers": 5,
    "timeout": 400
}

response = requests.post(url, json=payload)
result = response.json()

print(f"总计: {result['total']} 个问题")
print(f"成功: {result['success_count']} 个")
print(f"失败: {result['error_count']} 个")
print(f"耗时: {result['took']}ms")

for item in result['results']:
    print(f"\n问题 {item['id']}: {item['question']}")
    print(f"状态: {item['status']}")
    if item['status'] == 'success':
        print(f"答案: {item['answer'][:100]}...")
    else:
        print(f"错误: {item['error']}")
```

### cURL 示例

```bash
curl -X POST "http://localhost:8000/api/search/batch-query" \
  -H "Content-Type: application/json" \
  -d '{
    "questions": ["问题1", "问题2"],
    "api_url": "https://ai-aidq.sany.com.cn/v1/chat-messages",
    "jwt": "app-xxx",
    "jwt_chat": "Bearer xxx",
    "max_workers": 5,
    "timeout": 400
  }'
```

## 错误处理

### 请求错误

- **400 Bad Request**: 问题列表为空或参数验证失败
- **500 Internal Server Error**: 服务器内部错误

### 部分失败

即使部分问题处理失败，API 仍会返回 200 状态码，并在响应中标记每个问题的状态。通过 `success_count` 和 `error_count` 可以了解整体处理情况。

## 性能建议

1. **并发数设置**: 根据服务器性能和网络状况调整 `max_workers`，推荐值为 5-10
2. **超时设置**: 根据问题复杂度调整 `timeout`，复杂问题建议设置 400 秒以上
3. **批量大小**: 单次请求建议不超过 50 个问题，避免长时间等待

## 与原有批量脚本的对比

| 特性 | 原批量脚本 | 新 API |
|------|-----------|--------|
| 调用方式 | 命令行 + Excel | HTTP API + JSON |
| 输入方式 | 从 Excel 文件读取 | 通过请求体传入 |
| 输出方式 | 保存到文件 | 返回 JSON |
| 集成方式 | 独立运行 | 可集成到任何应用 |
| 实时性 | 需要轮询文件 | 实时获取结果 |

## 注意事项

1. JWT token 需要保持有效，过期后需要更新
2. 并发调用会增加服务器负载，请合理设置 `max_workers`
3. 部分问题失败不影响其他问题的处理
4. `answer` 字段是从 `response` 中提取的 `answer` 字段，如果响应中没有该字段会返回错误提示

