class StorageError(Exception):
    """Base class for storage-related errors"""
    pass


class CorruptionError(StorageError):
    """Raised when data corruption is detected"""
    pass
