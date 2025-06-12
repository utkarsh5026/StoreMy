"""
Primitive types and identifiers used throughout the database system.

This module contains basic types that have no dependencies on other parts
of the system, avoiding circular imports.
"""

from .transaction_id import TransactionId
from .page_id import PageId
from .record_id import RecordId
from .index_page_id import IndexPageId

__all__ = ["TransactionId", "PageId", "RecordId", "IndexPageId"]
