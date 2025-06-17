from enum import IntEnum
from dataclasses import dataclass
from typing import Dict, Any


class LogRecordType(IntEnum):
    ABORT_RECORD = 1
    COMMIT_RECORD = 2
    UPDATE_RECORD = 3
    BEGIN_RECORD = 4
    CHECKPOINT_RECORD = 5



@dataclass
class WALConfig:
    """Configuration for WAL system."""
    log_file_path: str = "database.log"
    checkpoint_interval: int = 100
    max_log_file_size: int = 1024 * 1024 * 100  # 100MB
    force_log_at_commit: bool = True
    enable_recovery: bool = True
    backup_log_files: bool = True
    corruption_detection: bool = True
    max_retry_attempts: int = 3
    retry_delay_ms: int = 100
    log_buffer_size: int = 4096  # 4KB buffer


class WALConstants:
    """Constants for WAL implementation."""
    NO_CHECKPOINT_ID = -1
    INT_SIZE = 4
    LONG_SIZE = 8
    PAGE_SIZE = 4096
    MAGIC_NUMBER = 0xDEADBEEF  # For corruption detection
    VERSION = 1

    LOG_FILE_HEADER_SIZE = 32
    RECORD_HEADER_SIZE = 20
    CHECKSUM_SIZE = 4

