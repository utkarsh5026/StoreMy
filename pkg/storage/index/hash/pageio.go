package hash

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
)

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
//   - Parses header (bucket number as uint64, entry count as int32, overflow page as uint64)
//   - Deserializes each entry
//   - Infers keyType from first entry's key
//   - Marks page as clean (not dirty)
func DeserializeHashPage(data []byte, pageID *page.PageDescriptor) (*HashPage, error) {
	if len(data) < hashHeaderSize {
		return nil, fmt.Errorf("invalid page data: too short")
	}

	buf := bytes.NewReader(data)

	var (
		bucketNumRaw uint64
		overflowPage primitives.PageNumber
		numEntries   int32
	)
	_ = binary.Read(buf, binary.BigEndian, &bucketNumRaw)
	_ = binary.Read(buf, binary.BigEndian, &numEntries)
	_ = binary.Read(buf, binary.BigEndian, &overflowPage)

	page := &HashPage{
		pageID:       pageID,
		bucketNum:    BucketNumber(bucketNumRaw),
		numEntries:   int(numEntries),
		overflowPage: overflowPage,
		entries:      make([]*index.IndexEntry, 0, numEntries),
		isDirty:      false,
	}

	for i := 0; i < int(numEntries); i++ {
		entry, err := index.DeserializeEntry(buf)
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
