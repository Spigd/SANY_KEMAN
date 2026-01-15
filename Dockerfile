FROM python:3.11-slim as builder

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 安装系统依赖（用于编译某些包）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 生产阶段
FROM python:3.11-slim as production

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 安装运行时依赖（用于健康检查等）
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 应用环境变量配置
# Elasticsearch配置
ENV ES_HOST=10.66.0.160 \
    ES_PORT=9200 \
    ES_INDEX_PREFIX=kman

# API配置
ENV API_HOST=0.0.0.0 \
    API_PORT=8082

# 数据文件配置
ENV METADATA_EXCEL_PATH=客满-元数据表.xlsx \
    METRIC_EXCEL_PATH=metric_latest.xlsx

# 元数据和指标API配置
ENV API_BASE_URL=http://localhost:8080 \
    METADATA_API_TIMEOUT=30 \
    METADATA_API_JWT=""

# API数据同步配置
ENV API_SYNC_ENABLED=false \
    API_SYNC_INTERVAL="" \
    API_DATA_DOMAIN_ID="" \
    API_METRIC_CATEGORY_ID=""

# 混合搜索权重配置
ENV ES_WEIGHT=1.0 \
    AC_WEIGHT=0.9 \
    SIM_WEIGHT=0.8

# 分词器配置
ENV DEFAULT_TOKENIZER=ik_max_word \
    DEFAULT_SEARCH_ANALYZER=ik_smart

# 日志配置
ENV LOG_LEVEL=INFO

# 数据库连接配置
ENV DB_TYPE=mysql \
    DB_HOST=10.70.40.134 \
    DB_PORT=3306 \
    DB_USER=root \
    DB_PASSWORD=Kb3LCNsM2Stp!d \
    DB_DATABASE=keman_data2 \
    DB_CHARSET=utf8mb4

# 维度值索引配置
ENV DIMENSION_VALUE_INDEXING_ENABLED=true \
    MAX_VALUES_PER_COLUMN=10000 \
    DIMENSION_BATCH_SIZE=100 \
    AUTO_EXTRACT_DIMENSIONS=true

# 创建非root用户
RUN groupadd -r appuser && useradd -r -g appuser appuser

# 从builder阶段复制Python包
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# 复制应用代码
COPY --chown=appuser:appuser . .

# 确保Excel文件权限正确
RUN chown -f appuser:appuser "客满-元数据表.xlsx" "metric_latest.xlsx" 2>/dev/null || true

# 创建日志目录
RUN mkdir -p /app/logs && chown -R appuser:appuser /app/logs

# 切换到非root用户
USER appuser

# 暴露端口
EXPOSE 8082

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8082/api/search/health || exit 1

# 启动命令
CMD ["python", "run.py"]