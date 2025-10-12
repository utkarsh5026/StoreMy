package page

import (
	"fmt"
	"os"
	"storemy/pkg/utils"
	"sync"
)

// BaseFile provides common file operations for all database file types.
// It handles file I/O, page counting, and thread-safety concerns.
type BaseFile struct {
	file   *os.File
	fileID int
	mutex  sync.RWMutex
}

// NewBaseFile creates a new base file handler
func NewBaseFile(filename string) (*BaseFile, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename cannot be empty")
	}

	file, err := utils.OpenFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return &BaseFile{
		file:   file,
		fileID: utils.HashString(file.Name()),
	}, nil
}

// GetID returns the unique identifier for this file
func (bf *BaseFile) GetID() int {
	return bf.fileID
}

// GetFile returns the underlying os.File (for subclass use)
func (bf *BaseFile) GetFile() *os.File {
	return bf.file
}

// NumPages returns the total number of pages in this file
func (bf *BaseFile) NumPages() (int, error) {
	bf.mutex.RLock()
	defer bf.mutex.RUnlock()

	if bf.file == nil {
		return 0, fmt.Errorf("file is closed")
	}

	fileInfo, err := bf.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	numPages := int(fileInfo.Size() / int64(PageSize))
	if fileInfo.Size()%int64(PageSize) != 0 {
		numPages++
	}

	return numPages, nil
}

// ReadPageData reads raw page data from disk at the specified page number
func (bf *BaseFile) ReadPageData(pageNo int) ([]byte, error) {
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

// WritePageData writes raw page data to disk at the specified page number
func (bf *BaseFile) WritePageData(pageNo int, pageData []byte) error {
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

// Close closes the underlying file
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
