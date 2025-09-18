package storage

import (
	"storemy/pkg/tuple"
	"sync/atomic"
)

const (
	// PageSize is the size of each page in bytes (4KB)
	PageSize = 4096
)

type TransactionID struct {
	id int64
}

var transactionCounter int64

// NewTransactionID creates a new unique transaction ID
func NewTransactionID() *TransactionID {
	return &TransactionID{
		id: atomic.AddInt64(&transactionCounter, 1),
	}
}

func (tid *TransactionID) GetID() int64 {
	return tid.id
}

// Equals checks if two transaction IDs are equal
func (tid *TransactionID) Equals(other *TransactionID) bool {
	if other == nil {
		return false
	}
	return tid.id == other.id
}

// Page interface represents a page that is resident in the buffer pool
// Pages may be "dirty", indicating they have been modified since last written to disk
type Page interface {
	// GetID returns the ID of this page
	GetID() tuple.PageID

	// IsDirty returns the transaction ID that last dirtied this page, or nil if clean
	IsDirty() *TransactionID

	// MarkDirty sets the dirty state of this page
	MarkDirty(dirty bool, tid *TransactionID)

	// GetPageData returns a byte array representing the contents of this page
	// Used to serialize this page to disk
	GetPageData() []byte

	// GetBeforeImage returns a representation of this page before modifications
	// Used by recovery
	GetBeforeImage() Page

	// SetBeforeImage copies current content to the before image
	// Called when a transaction that wrote this page commits
	SetBeforeImage()
}
