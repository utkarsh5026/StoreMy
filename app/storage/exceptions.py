class IOException(Exception):
    pass


class PageValidationError(Exception):
    """Base class for page validation errors."""
    pass


class InvalidPageTypeError(PageValidationError):
    """Raised when a page is not of the expected type."""
    pass


class InvalidPageSizeError(PageValidationError):
    """Raised when page size doesn't match expected size."""
    pass


class InvalidPageHeaderError(PageValidationError):
    """Raised when page header contains invalid values."""
    pass


class InvalidSlotError(PageValidationError):
    """Raised when page slots contain invalid data."""
    pass


class TupleValidationError(PageValidationError):
    """Raised when tuple data is inconsistent or invalid."""
    pass


class PageCorruptionError(PageValidationError):
    """Raised when page appears to be corrupted."""
    pass
