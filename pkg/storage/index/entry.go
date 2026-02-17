package index

import (
	"bytes"
	"encoding/binary"
	"io"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
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
func (i *IndexEntry) Serialize(w io.Writer) error {
	rid := i.RID
	if err := binary.Write(w, binary.BigEndian, byte(i.Key.Type())); err != nil {
		return err
	}
	if err := i.Key.Serialize(w); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, rid.PageID.FileID()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, rid.PageID.PageNo()); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, rid.TupleNum)
}

// DeserializeEntry reads a single hash entry from the byte stream.
//
// Expects format:
//  1. Key field (type byte + data)
//  2. Table ID (4 bytes)
//  3. Page number (4 bytes)
//  4. Tuple number (4 bytes)
//
// Parameters:
//   - r: Reader to deserialize from
//
// Returns:
//   - Reconstructed index.IndexEntry
//   - Error if read fails or format is invalid
func DeserializeEntry(r *bytes.Reader) (*IndexEntry, error) {
	key, err := deserializeField(r)
	if err != nil {
		return nil, err
	}

	var tableID primitives.FileID
	var pageNum primitives.PageNumber
	var tupleNum primitives.SlotID

	if err := binary.Read(r, binary.BigEndian, &tableID); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &pageNum); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &tupleNum); err != nil {
		return nil, err
	}

	pageID := page.NewPageDescriptor(tableID, pageNum)
	rid := tuple.NewTupleRecordID(pageID, tupleNum)

	return NewIndexEntry(key, rid), nil
}

// deserializeField reads a field value from the byte stream.
//
// Expects format:
//  1. Type byte (IntType, StringType, BoolType, FloatType)
//  2. Type-specific data
//
// Parameters:
//   - r: Reader to deserialize from
//
// Returns:
//   - Reconstructed Field
//   - Error if read fails or type is invalid
func deserializeField(r *bytes.Reader) (types.Field, error) {
	fieldTypeByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	fieldType := types.Type(fieldTypeByte)
	return types.ParseField(r, fieldType)
}
