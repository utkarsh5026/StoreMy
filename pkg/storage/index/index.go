package index

import (
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type TID = *primitives.TransactionID
type RecID = *tuple.TupleRecordID
type Field = types.Field

type IndexType int

const (
	BTreeIndex IndexType = iota
	HashIndex
)

// Index is the generic interface that all index types must implement
// This allows different index implementations (BTree, Hash, etc.) to be used interchangeably
type Index interface {
	// Insert adds a key-value pair to the index
	// key: The indexed field value
	// rid: The location of the tuple containing this key
	Insert(tid TID, key Field, rid RecID) error

	// Delete removes a key-value pair from the index
	Delete(tid TID, key Field, rid RecID) error

	// Search finds all tuple locations for a given key
	// Returns empty slice if key not found
	Search(tid TID, key Field) ([]RecID, error)

	// RangeSearch finds all tuples where key is in [startKey, endKey]
	// Used for range queries like: WHERE age >= 18 AND age <= 65
	RangeSearch(tid TID, startKey, endKey Field) ([]RecID, error)

	// GetIndexType returns the type of this index
	GetIndexType() IndexType

	// GetKeyType returns the type of keys this index handles
	GetKeyType() types.Type

	// Close releases resources held by the index
	Close() error
}

// IndexFile extends Index with file persistence capabilities
type IndexFile interface {
	Index

	// GetID returns unique identifier for this index file
	GetID() int

	// ReadPage reads a specific page from the index file
	ReadPage(pid primitives.PageID) (page.Page, error)

	// WritePage writes a page to the index file
	WritePage(p page.Page) error

	// NumPages returns the number of pages in the index file
	NumPages() int
}

// IndexMetadata stores information about an index
type IndexMetadata struct {
	IndexName, FieldNam, FilePath, FieldName string
	IndexID, TableID, FieldIndex             int
	KeyType                                  types.Type
	IndexType                                IndexType
	IsUnique, IsPrimary                      bool
}
