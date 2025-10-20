package index

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

type TID = *primitives.TransactionID
type RecID = *tuple.TupleRecordID
type Field = types.Field

type IndexType string

const (
	BTreeIndex IndexType = "BTREE"
	HashIndex  IndexType = "HASH"
)

func ParseIndexType(str string) (IndexType, error) {
	switch strings.ToUpper(str) {

	case "BTREE":
		return BTreeIndex, nil

	case "HASH":
		return HashIndex, nil

	default:
		return "", fmt.Errorf("error in parsing the index type ")
	}
}

// IndexEntry represents a single key-value pair in a hash bucket.
// Maps an index key to a tuple location in the heap file.
//
// Structure:
//   - Key: The indexed field value (can be Int, String, Bool, or Float)
//   - RID: Record ID pointing to the tuple in the heap file
type IndexEntry struct {
	Key Field
	RID RecID
}

// NewHashEntry creates a new hash entry with the given key and tuple location.
//
// Parameters:
//   - key: Field value to index
//   - rid: Tuple record ID pointing to the actual data
//
// Returns a new IndexEntry ready to be inserted into a hash page.
func NewIndexEntry(key Field, rid RecID) *IndexEntry {
	return &IndexEntry{
		Key: key,
		RID: rid,
	}
}

// Equals checks if this hash entry is identical to another.
// Compares both the key value and the tuple location.
//
// Parameters:
//   - other: IndexEntry to compare against
//
// Returns true if both key and RID match exactly.
func (i *IndexEntry) Equals(other *IndexEntry) bool {
	return i.Key.Equals(other.Key) &&
		i.RID.PageID.Equals(other.RID.PageID) &&
		i.RID.TupleNum == other.RID.TupleNum
}

// Index is the generic interface that all index types must implement
// This allows different index implementations (BTree, Hash, etc.) to be used interchangeably
type Index interface {
	// Insert adds a key-value pair to the index
	// key: The indexed field value
	// rid: The location of the tuple containing this key
	Insert(key Field, rid RecID) error

	// Delete removes a key-value pair from the index
	Delete(key Field, rid RecID) error

	// Search finds all tuple locations for a given key
	// Returns empty slice if key not found
	Search(key Field) ([]RecID, error)

	// RangeSearch finds all tuples where key is in [startKey, endKey]
	// Used for range queries like: WHERE age >= 18 AND age <= 65
	RangeSearch(startKey, endKey Field) ([]RecID, error)

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
