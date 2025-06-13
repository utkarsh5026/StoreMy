import time
from app.storage.interfaces import Page


class PageEntry:
    """Represents a cached page with metadata."""

    def __init__(self, page: Page, page_type: str):
        self.page = page
        self.page_type = page_type
        self.access_count = 0
        self.last_access_time = time.time()
        self.pin_count = 0
        self.is_pinned = False
        self.load_time = time.time()

    def touch(self) -> None:
        """Update access statistics."""
        self.access_count += 1
        self.last_access_time = time.time()

    def pin(self) -> None:
        """Pin the page in memory."""
        self.pin_count += 1
        self.is_pinned = True

    def unpin(self) -> None:
        """Unpin the page."""
        self.pin_count = max(0, self.pin_count - 1)
        if self.pin_count == 0:
            self.is_pinned = False

    def can_evict(self) -> bool:
        """Check if the page can be evicted."""
        return not self.is_pinned and not self.page.is_dirty()

