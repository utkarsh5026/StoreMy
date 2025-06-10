from enum import Enum


class Permissions(Enum):
    """
    Class representing requested permissions to a relation/file.

    This enum defines the two levels of permission for accessing pages:
    - READ_ONLY: Can read the page but not modify it
    - READ_WRITE: Can both read and modify the page

    The lock manager uses these permissions to determine what
    type of lock to acquire (shared vs. exclusive).
    """

    READ_ONLY = 0
    READ_WRITE = 1

    def is_read_write(self) -> bool:
        """Return True if this permission allows writing."""
        return self == Permissions.READ_WRITE

    def is_read_only(self) -> bool:
        """Return True if this permission is read-only."""
        return self == Permissions.READ_ONLY

    def __str__(self) -> str:
        return "READ_WRITE" if self.is_read_write() else "READ_ONLY"
