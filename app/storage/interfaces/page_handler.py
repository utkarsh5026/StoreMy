from abc import ABC, abstractmethod
from typing import Any
from app.primitives import PageId
from app.storage.interfaces import Page


class PageHandler(ABC):
    """Abstract base class for page handlers."""

    @abstractmethod
    def load_page(self, page_id: PageId) -> Page:
        """Load a page from disk."""
        pass

    @abstractmethod
    def write_page(self, page: Page) -> None:
        """Write a page to disk."""
        pass

    @abstractmethod
    def validate_page(self, page: Page) -> bool:
        """Validate page structure and contents."""
        pass

    @abstractmethod
    def get_page_info(self, page: Page) -> dict[str, Any]:
        """Get diagnostic information about a page."""
        pass
