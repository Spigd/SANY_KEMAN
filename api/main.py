"""
FastAPI主应用 - V3版本
"""

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.config import config
from .search_api import router as search_router

# 配置日志
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# 创建FastAPI应用
app = FastAPI(
    title="元数据搜索系统 V3",
    description="""
    ## 元数据搜索系统 V3 - 混合检索增强版
    
    ### 🚀 新特性
    - **混合检索**: 结合Elasticsearch、AC自动机、相似度匹配三种搜索算法
    - **分词控制**: 支持开启/关闭分词，适应不同搜索场景
    - **一键部署**: 创建索引时自动加载数据，无需手动操作
    - **智能搜索**: 自动选择最优搜索策略
    
    ### 📊 支持的搜索方法
    - **hybrid**: 混合搜索（推荐）
    - **elasticsearch**: Elasticsearch全文搜索
    - **ac_matcher**: AC自动机精确匹配
    - **similarity**: 相似度匹配
    
    ### 🔧 分词控制
    - **use_tokenization=true**: 启用分词，适合复杂查询
    - **use_tokenization=false**: 精确匹配，适合专业术语
    
    ### 📝 使用流程
    1. 调用 `/api/search/index/create` 创建索引并加载数据
    2. 使用 `/api/search/fields` 进行搜索
    3. 通过 `/api/search/stats` 查看系统状态
    """,
    version="3.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境中应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(
    search_router,
    prefix="/api/search",
    tags=["搜索"]
)

@app.get("/", summary="根路径")
async def root():
    """根路径信息"""
    return {
        "name": "元数据搜索系统 V3",
        "version": "3.0.0",
        "description": "混合检索增强版元数据搜索系统",
        "features": [
            "混合检索",
            "分词控制",
            "一键部署",
            "智能搜索"
        ],
        "docs_url": "/docs",
        "api_prefix": "/api/search"
    }

@app.get("/version", summary="版本信息")
async def get_version():
    """获取版本信息"""
    return {
        "version": "3.0.0",
        "name": "es_search_system_v3",
        "build_date": "2024-01-01",
        "python_version": "3.8+",
        "dependencies": {
            "elasticsearch": "8.x",
            "fastapi": "0.100+",
            "pandas": "2.x",
            "ahocorasick": "2.x"
        }
    }

# 启动事件
@app.on_event("startup")
async def startup_event():
    """应用启动事件"""
    logger.info("🚀 元数据搜索系统 V3 启动中...")
    logger.info(f"📊 配置信息:")
    logger.info(f"  - Elasticsearch: {config.elasticsearch_url}")
    logger.info(f"  - 索引名称: {config.metadata_index_name}")
    logger.info(f"  - API端口: {config.API_PORT}")
    logger.info(f"  - 默认分词器: {config.DEFAULT_TOKENIZER}")
    logger.info(f"  - 数据库地址: {config.DATABASE_CONFIGS['default']['host']}")

    logger.info("✅ 系统启动完成！")

# 关闭事件
@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭事件"""
    logger.info("👋 元数据搜索系统 V3 正在关闭...")
    logger.info("✅ 系统已安全关闭！") 