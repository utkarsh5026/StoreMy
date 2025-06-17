from app.core.exceptions import DbException


class WALException(DbException):
    """Base exception for WAL-related errors."""
    pass


class LogFileCorruptedException(WALException):
    """Raised when log file corruption is detected."""
    def __init__(self, message: str, offset: int = -1, checksum_expected: int = None, checksum_actual: int = None):
        super().__init__(message)
        self.offset = offset
        self.checksum_expected = checksum_expected
        self.checksum_actual = checksum_actual


class LogRecordInvalidException(WALException):
    """Raised when a log record is invalid."""
    pass


class LogFileFullException(WALException):
    """Raised when log file is full and cannot be extended."""
    pass


class RecoveryFailedException(WALException):
    """Raised when recovery fails."""
    pass


class CheckpointFailedException(WALException):
    """Raised when checkpoint creation fails."""
    pass


class TransactionLogException(WALException):
    """Rose for transaction logging errors."""
    pass