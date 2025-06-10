from .file import HeapFile
from .page import HeapPage, HeapPageId


def create_empty_heap_file(file_path: str, tuple_desc, num_pages: int = 1):
    """
    Create a new empty HeapFile with the specified number of empty pages.

    This utility function is useful for testing and initialization.

    Args:
        file_path: Path where the file should be created
        tuple_desc: Schema for the table
        num_pages: Number of empty pages to create

    Returns:
        HeapFile instance for the created file
    """

    # Create the file
    heap_file = HeapFile(file_path, tuple_desc)

    # Create empty pages
    for page_num in range(num_pages):
        page_id = HeapPageId(heap_file.get_id(), page_num)
        empty_page = HeapPage(page_id, bytes(HeapFile.PAGE_SIZE))
        heap_file.write_page(empty_page)

    return heap_file


def calculate_page_capacity(tuple_desc) -> int:
    """
    Calculate how many tuples can fit on a single page.

    This calculation accounts for:
    1. Header space (bitmap for tracking which slots are used)
    2. Tuple space (fixed size per tuple based on schema)

    Args:
        tuple_desc: The schema description

    Returns:
        Maximum number of tuples that can fit on one page
    """
    PAGE_SIZE = 4096  # Standard page size

    tuple_size = tuple_desc.get_size()

    # Each tuple needs 1 bit in the header + tuple_size bytes for data
    # Header is stored as bytes, so we need ceiling(num_tuples / 8) bytes for header

    # Solve: header_bytes + (num_tuples * tuple_size) <= PAGE_SIZE
    # Where: header_bytes = ceiling(num_tuples / 8)

    # This is the formula from HeapPage calculation
    bits_per_tuple = tuple_size * 8 + 1  # 8 bits per byte + 1 bit for header
    max_tuples = (PAGE_SIZE * 8) // bits_per_tuple

    return max_tuples


def verify_file_integrity(heap_file) -> list[str]:
    """
    Verify the integrity of a HeapFile by checking all pages.

    This utility function can be used for debugging and testing.

    Args:
        heap_file: The HeapFile to verify

    Returns:
        List of error messages (empty if file is valid)
    """
    errors = []

    try:
        num_pages = heap_file.num_pages()

        for page_num in range(num_pages):
            try:
                page_id = heap_file.HeapPageId(heap_file.get_id(), page_num)
                page = heap_file.read_page(page_id)

                # Verify page data
                if len(page.get_page_data()) != HeapFile.PAGE_SIZE:
                    errors.append(f"Page {page_num}: incorrect size")

                # Try to iterate through tuples
                tuple_count = 0
                for tuple_data in page:
                    tuple_count += 1
                    if tuple_data.get_tuple_desc() != heap_file.get_tuple_desc():
                        errors.append(
                            f"Page {page_num}: tuple schema mismatch")

            except Exception as e:
                errors.append(f"Page {page_num}: {str(e)}")

    except Exception as e:
        errors.append(f"File-level error: {str(e)}")

    return errors


def format_size(bytes_size: int) -> str:
    """
    Format a byte size as a human-readable string.

    Args:
        bytes_size: Size in bytes

    Returns:
        Formatted string like "1.5 KB" or "2.3 MB"
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} TB"
