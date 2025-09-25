package page

import (
	"storemy/pkg/transaction"
	"storemy/pkg/tuple"
)

const (
	// PageSize is the size of each page in bytes (4KB)
	PageSize = 4096
)

// Page interface represents a page that is resident in the buffer pool
// Pages may be "dirty", indicating they have been modified since last written to disk
type Page interface {
	// GetID returns the ID of this page
	GetID() tuple.PageID

	// IsDirty returns the transaction ID that last dirtied this page, or nil if clean
	IsDirty() *transaction.TransactionID

	// MarkDirty sets the dirty state of this page
	MarkDirty(dirty bool, tid *transaction.TransactionID)

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
