package index

import (
	"bytes"
	"encoding/binary"
)

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

// Serialize converts the IndexEntry into a byte array for disk storage.
// This is the primary method for persisting hash index entries to pages.
//
// Format (byte layout):
//  1. Key type (1 byte): Field type identifier
//  2. Key data (variable length): Serialized field value
//  3. Table ID (4 bytes, big-endian): Table identifier from RID
//  4. Page number (4 bytes, big-endian): Page location in heap file
//  5. Tuple number (4 bytes, big-endian): Slot number within the page
//
// Returns:
//   - []byte: Serialized byte representation of the entry
//   - error: nil (currently no error cases, included for future compatibility)
//
// Example:
//
//	entry := NewIndexEntry(IntField{Value: 42}, rid)
//	data, err := entry.Serialize()
func (i *IndexEntry) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)
	rid := i.RID
	binary.Write(buf, binary.BigEndian, byte(i.Key.Type()))
	i.Key.Serialize(buf)
	binary.Write(buf, binary.BigEndian, rid.PageID.GetTableID())
	binary.Write(buf, binary.BigEndian, rid.PageID.PageNo())
	binary.Write(buf, binary.BigEndian, rid.TupleNum)
	return buf.Bytes(), nil
}
