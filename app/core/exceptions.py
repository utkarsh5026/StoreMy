"""Custom exceptions for the database system."""


class DbException(Exception):
    """Base exception for database-related errors."""
    pass


class TransactionAbortedException(Exception):
    """Raised when a transaction must be aborted (e.g., due to deadlock)."""
    pass


class ParsingException(Exception):
    """Raised when SQL parsing fails."""
    pass
