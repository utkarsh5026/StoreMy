package hash

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// serializeEntry writes a single hash entry to the output stream.
// Internal method used by GetPageData for disk serialization.
//
// Format:
//  1. Key field (variable length based on type)
//  2. Table ID (4 bytes, big-endian int32)
//  3. Page number (4 bytes, big-endian int32)
//  4. Tuple number (4 bytes, big-endian int32)
//
// Parameters:
//   - w: Writer to serialize to
//   - entry: index.IndexEntry to serialize
//
// Returns error if serialization fails.
func (hp *HashPage) serializeEntry(w io.Writer, entry *index.IndexEntry) error {
	if err := hp.serializeField(w, entry.Key); err != nil {
		return err
	}

	rid := entry.RID
	binary.Write(w, binary.BigEndian, rid.PageID.GetTableID())
	binary.Write(w, binary.BigEndian, rid.PageID.PageNo())
	binary.Write(w, binary.BigEndian, rid.TupleNum)

	return nil
}

// serializeField writes a field value to the output stream.
// Prefixes with type byte for deserialization.
//
// Format:
//  1. Type byte (1 byte)
//  2. Field data (variable length based on type)
//
// Parameters:
//   - w: Writer to serialize to
//   - field: Field to serialize
//
// Returns error if serialization fails.
func (hp *HashPage) serializeField(w io.Writer, field types.Field) error {
	binary.Write(w, binary.BigEndian, byte(field.Type()))
	return field.Serialize(w)
}

// DeserializeHashPage reconstructs a hash page from serialized bytes.
// Reads header and all entries from the byte array.
//
// Parameters:
//   - data: Serialized page data (should be PageSize bytes)
//   - pageID: Page identifier to assign to deserialized page
//
// Returns:
//   - Reconstructed HashPage with all entries
//   - Error if data is corrupted or too short
//
// Behavior:
//   - Parses header (bucket, count, overflow)
//   - Deserializes each entry
//   - Infers keyType from first entry's key
//   - Marks page as clean (not dirty)
func DeserializeHashPage(data []byte, pageID *page.PageDescriptor) (*HashPage, error) {
	if len(data) < hashHeaderSize {
		return nil, fmt.Errorf("invalid page data: too short")
	}

	buf := bytes.NewReader(data)

	// Read header
	var bucketNum, numEntries, overflowPage int32
	binary.Read(buf, binary.BigEndian, &bucketNum)
	binary.Read(buf, binary.BigEndian, &numEntries)
	binary.Read(buf, binary.BigEndian, &overflowPage)

	page := &HashPage{
		pageID:       pageID,
		bucketNum:    int(bucketNum),
		numEntries:   int(numEntries),
		overflowPage: int(overflowPage),
		entries:      make([]*index.IndexEntry, 0, numEntries),
		isDirty:      false,
	}

	for i := 0; i < int(numEntries); i++ {
		entry, err := deserializeHashEntry(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize entry %d: %w", i, err)
		}
		page.entries = append(page.entries, entry)
		if i == 0 && entry.Key != nil {
			page.keyType = entry.Key.Type()
		}
	}

	return page, nil
}

// deserializeHashEntry reads a single hash entry from the byte stream.
// Internal method used by DeserializeHashPage.
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
func deserializeHashEntry(r *bytes.Reader) (*index.IndexEntry, error) {
	key, err := deserializeField(r)
	if err != nil {
		return nil, err
	}

	var tableID primitives.TableID
	var pageNum primitives.PageNumber
	var tupleNum primitives.SlotID

	binary.Read(r, binary.BigEndian, &tableID)
	binary.Read(r, binary.BigEndian, &pageNum)
	binary.Read(r, binary.BigEndian, &tupleNum)

	pageID := page.NewPageDescriptor(tableID, pageNum)
	rid := tuple.NewTupleRecordID(pageID, tupleNum)

	return index.NewIndexEntry(key, rid), nil
}

// deserializeField reads a field value from the byte stream.
// Internal method used by deserializeHashEntry.
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
