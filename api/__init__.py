"""
API模块
"""

from .main import app
from .search_api import router as search_router

__all__ = [
    'app',
    'search_router'
] 