"""
Interfaces for the storage system.

This module contains interfaces for the storage system,
which are used to abstract the underlying storage implementation.
"""

from .db_file import DbFile
from .page_id import PageId
from .page import Page
from .page_handler import PageHandler

__all__ = ['DbFile', 'PageId', 'Page', 'PageHandler']
