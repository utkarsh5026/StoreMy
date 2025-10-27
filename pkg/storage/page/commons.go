package page

import (
	"fmt"
	"os"
	"storemy/pkg/primitives"
	"sync"
)

// BaseFile provides common file operations for all database file types.
// It handles file I/O, page counting, and thread-safety concerns.
//
// BaseFile serves as the foundational layer for managing database files in the StoreMy system.
// It provides thread-safe operations for reading and writing pages, tracking file metadata,
// and ensuring data consistency through proper synchronization primitives.
//
// Key responsibilities:
//   - Managing the underlying OS file handle
//   - Providing thread-safe read/write operations
//   - Calculating and tracking page counts
//   - Generating unique file identifiers
//
// Thread-safety: All public methods use read/write locks to ensure safe concurrent access.
type BaseFile struct {
	file     *os.File            // The underlying OS file handle for I/O operations
	fileID   primitives.FileID   // Unique identifier generated from the file path hash
	mutex    sync.RWMutex        // Read-write mutex for thread-safe operations
	filePath primitives.Filepath // Absolute path to the database file
}

// NewBaseFile creates a new base file handler.
//
// This function opens a database file and initializes the BaseFile structure.
// It generates a unique file ID based on the file path hash and prepares
// the file for subsequent page-based operations.
//
// Parameters:
//   - filename: The path to the database file to open
//
// Returns:
//   - *BaseFile: A pointer to the initialized BaseFile structure
//   - error: An error if the filename is empty or file opening fails
func NewBaseFile(filePath primitives.Filepath) (*BaseFile, error) {
	if filePath == "" {
		return nil, fmt.Errorf("filePath cannot be empty")
	}

	file, err := openFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return &BaseFile{
		file:     file,
		fileID:   filePath.Hash(),
		filePath: filePath,
	}, nil
}

// GetID returns the unique identifier for this file.
//
// The file ID is generated using a hash of the file path and remains
// constant throughout the lifetime of the BaseFile instance.
//
// Returns:
//   - int: The unique file identifier
func (bf *BaseFile) GetID() primitives.FileID {
	return bf.fileID
}

// GetFile returns the underlying os.File handle.
//
// This method is primarily intended for use by subclasses that need
// direct access to the file handle for specialized operations.
//
// Returns:
//   - *os.File: The underlying OS file handle
//
// Note: Direct manipulation of the file handle should be done carefully
// to avoid breaking the thread-safety guarantees provided by BaseFile.
func (bf *BaseFile) GetFile() *os.File {
	return bf.file
}

// NumPages returns the total number of pages in this file.
//
// This method calculates the number of pages by dividing the file size
// by the page size. If the file size is not evenly divisible by the page
// size, it rounds up to include the partial page.
//
// Returns:
//   - int: The total number of pages in the file
//   - error: An error if the file is closed or stat operation fails
//
// Thread-safety: Uses read lock to allow concurrent reads while preventing
// writes during the operation.
func (bf *BaseFile) NumPages() (primitives.PageNumber, error) {
	bf.mutex.RLock()
	defer bf.mutex.RUnlock()

	if bf.file == nil {
		return 0, fmt.Errorf("file is closed")
	}

	fileInfo, err := bf.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	numPages := primitives.PageNumber(fileInfo.Size() / int64(PageSize))
	if fileInfo.Size()%int64(PageSize) != 0 {
		numPages++
	}

	return numPages, nil
}

// ReadPageData reads raw page data from disk at the specified page number.
//
// This method reads exactly PageSize bytes from the file at the offset
// corresponding to the given page number. The data is returned as a byte slice.
//
// Parameters:
//   - pageNo: The zero-based page number to read
//
// Returns:
//   - []byte: A slice containing the raw page data (always PageSize bytes)
//   - error: An error if the file is closed or read operation fails
func (bf *BaseFile) ReadPageData(pageNo primitives.PageNumber) ([]byte, error) {
	bf.mutex.RLock()
	defer bf.mutex.RUnlock()

	if bf.file == nil {
		return nil, fmt.Errorf("file is closed")
	}

	offset := int64(pageNo) * int64(PageSize)
	pageData := make([]byte, PageSize)

	_, err := bf.file.ReadAt(pageData, offset)
	return pageData, err
}

// WritePageData writes raw page data to disk at the specified page number.
//
// This method writes exactly PageSize bytes to the file at the offset
// corresponding to the given page number. After writing, it syncs the
// file to ensure data is persisted to disk.
//
// Parameters:
//   - pageNo: The zero-based page number to write
//   - pageData: The raw page data to write (must be exactly PageSize bytes)
//
// Returns:
//   - error: An error if the file is closed, data size is invalid, or I/O fails
//
// Thread-safety: Uses write lock to ensure exclusive access during write.
//
// Note: This method calls Sync() to ensure durability, which may impact
// performance for high-frequency writes. Consider batching writes when possible.
//
// Example:
//
//	data := make([]byte, PageSize)
//	// Fill data...
//	err := bf.WritePageData(5, data)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (bf *BaseFile) WritePageData(pageNo primitives.PageNumber, pageData []byte) error {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	if bf.file == nil {
		return fmt.Errorf("file is closed")
	}

	if len(pageData) != PageSize {
		return fmt.Errorf("invalid page data size: expected %d, got %d", PageSize, len(pageData))
	}

	offset := int64(pageNo) * int64(PageSize)

	if _, err := bf.file.WriteAt(pageData, offset); err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

	if err := bf.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// Close closes the underlying file handle.
//
// This method should be called when the BaseFile is no longer needed
// to properly release system resources. After calling Close, all other
// methods will return errors.
//
// Returns:
//   - error: An error if the close operation fails, or nil if already closed
//
// Thread-safety: Uses write lock to ensure no operations are in progress.
//
// Example:
//
//	bf, _ := NewBaseFile("database.db")
//	defer bf.Close()
func (bf *BaseFile) Close() error {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	if bf.file != nil {
		err := bf.file.Close()
		bf.file = nil
		return err
	}

	return nil
}

// AllocateNewPage atomically allocates and reserves the next available page number.
// This method ensures that concurrent calls to allocate new pages receive
// unique page numbers, preventing race conditions during concurrent inserts.
//
// The allocation is atomic - it:
//  1. Reads the current page count
//  2. Extends the file by one page (writes zeros) to reserve the space
//  3. Returns the allocated page number
//
// All operations occur while holding the write lock, ensuring no other
// thread can see an intermediate state or allocate the same page number.
//
// Returns:
//   - int: The allocated page number (equal to old NumPages)
//   - error: An error if the file is closed, stat fails, or write fails
//
// Thread-safety: Uses write lock to ensure exclusive access during allocation.
//
// Note: After allocation, the caller should overwrite the zero-filled page
// with actual data using WritePageData. The zero-fill ensures the file size
// increases atomically, preventing other threads from allocating the same page.
func (bf *BaseFile) AllocateNewPage() (primitives.PageNumber, error) {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	if bf.file == nil {
		return 0, fmt.Errorf("file is closed")
	}

	fileInfo, err := bf.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	// Calculate current number of pages
	currentSize := fileInfo.Size()
	numPages := int(currentSize / int64(PageSize))
	if currentSize%int64(PageSize) != 0 {
		numPages++
	}

	// Allocate page number (this will be the new page)
	allocatedPageNo := numPages

	// Atomically reserve space by writing a zero-filled page
	// This ensures the file size increases immediately, preventing
	// other threads from allocating the same page number
	zeroPage := make([]byte, PageSize)
	offset := int64(allocatedPageNo) * int64(PageSize)

	if _, err := bf.file.WriteAt(zeroPage, offset); err != nil {
		return 0, fmt.Errorf("failed to reserve page space: %w", err)
	}

	// Sync to ensure the size change is visible to other processes
	if err := bf.file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync file after page allocation: %w", err)
	}

	return primitives.PageNumber(allocatedPageNo), nil
}

// FilePath returns the absolute path to the database file.
//
// Returns:
//   - string: The file path used to create this BaseFile instance
func (bf *BaseFile) FilePath() primitives.Filepath {
	return bf.filePath
}

func openFile(filename primitives.Filepath) (*os.File, error) {
	file, err := os.OpenFile(string(filename), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", filename, err)
	}
	return file, nil
}
