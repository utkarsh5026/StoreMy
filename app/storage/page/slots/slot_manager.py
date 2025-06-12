from abc import ABC, abstractmethod
from typing import Optional


class SlotManager(ABC):
    """Abstract slot allocation strategy."""

    @abstractmethod
    def allocate_slot(self) -> Optional[int]:
        """Allocate a free slot."""
        pass

    @abstractmethod
    def deallocate_slot(self, slot_number: int) -> None:
        """Free a slot."""
        pass

    @abstractmethod
    def is_slot_used(self, slot_number: int) -> bool:
        """Check if slot is in use."""
        pass

    @abstractmethod
    def get_free_slot_count(self) -> int:
        """Get number of free slots."""
        pass