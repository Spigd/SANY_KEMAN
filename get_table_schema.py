import requests
import json

def get_schema_info(base_url: str, datasource_id=None, table_names='', result: dict = None) -> dict:
    """获取数据库Schema信息"""

    if result is None:
        result = init_result_format('schema_only')

    try:
        params = {}
        if datasource_id:
            params['datasource_id'] = datasource_id
        if table_names:
            params['table_names'] = table_names
        params['include_samples'] = 'true'

        response = requests.get(f"{base_url}/text2sql/schema", params=params, timeout=30)

        if response.status_code != 200:
            result["error_message"] = f"HTTP {response.status_code}: {response.text}"
            return result

        api_result = response.json()
        if api_result['code'] != 200:
            result["error_message"] = api_result['message']
            return result

        schema_data = api_result['data']

        # 格式化Schema信息
        formatted_schema = format_schema_for_dify(schema_data)

        # 填充返回结果
        result["success"] = 1
        result["data_content"] = formatted_schema
        result["summary_info"] = f"数据库: {schema_data['database_info']['database_name']}, 表数量: {schema_data['table_count']}, 总列数: {schema_data['total_columns']}"
        result["recommendation"] = f"成功获取到 {schema_data['table_count']} 个表的Schema信息，可用于Text2SQL查询生成。"

        return result

    except Exception as e:
        result["error_message"] = f"获取Schema失败: {str(e)}"
        return result

def format_schema_for_dify(schema_data: dict) -> str:
    """将Schema数据格式化为Dify友好的文本格式"""

    lines = []
    lines.append(f"📊 数据库: {schema_data['database_info']['database_name']}")
    lines.append(f"🏢 数据源: {schema_data['database_info']['datasource_name']}")
    lines.append(f"📋 表数量: {schema_data['table_count']}")
    lines.append("")

    for i, table in enumerate(schema_data['tables'], 1):
        lines.append(f"## {i}. 表名: {table['table_name']}")
        if table['table_comment']:
            lines.append(f"   说明: {table['table_comment']}")

        lines.append("   字段列表:")
        for col in table['columns']:
            nullable = "可空" if col['is_nullable'] else "必填"
            comment_text = f" - {col['column_comment']}" if col['column_comment'] else ""
            lines.append(f"   • {col['column_name']} ({col['data_type']}) [{nullable}]{comment_text}")

        lines.append("")

    return "\n".join(lines)

def init_result_format(mode: str) -> dict:
    """初始化统一返回格式"""
    return {
        "success": 0,
        "error_message": "",
        "mode": mode,
        "data_content": "",
        "summary_info": "",
        "recommendation": ""
    }

def main(args: str, base_url: str, table: str) -> dict:
    args = json.loads(args)

    base_url = base_url

    # 获取输入参数
    datasource_id = args.get('datasource_id')
    table_names_list = table
    table_names = ",".join(table_names_list)

    result = get_schema_info(base_url, datasource_id, table_names)

    return result