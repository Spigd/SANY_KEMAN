#!/usr/bin/env python3
"""
启动脚本 - 元数据搜索系统 V4
"""

import uvicorn
from core.config import config

if __name__ == "__main__":
    uvicorn.run(
        "api.main:app",
        host=config.API_HOST,
        port=config.API_PORT,
        reload=True,
        log_level=config.LOG_LEVEL.lower()
    )