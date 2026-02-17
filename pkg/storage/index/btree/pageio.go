package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// DeserializeBTreePage reads a page from bytes
func DeserializeBTreePage(data []byte, pageID *page.PageDescriptor) (*BTreePage, error) {
	if len(data) < headerSize {
		return nil, fmt.Errorf("invalid page data: too short")
	}

	buf := bytes.NewReader(data)

	pageType, _ := buf.ReadByte()

	var ParentPage, NextLeaf, PrevLeaf uint64
	var numEntries int32
	_ = binary.Read(buf, binary.BigEndian, &ParentPage)
	_ = binary.Read(buf, binary.BigEndian, &numEntries)
	_ = binary.Read(buf, binary.BigEndian, &NextLeaf)
	_ = binary.Read(buf, binary.BigEndian, &PrevLeaf)

	btreePage := &BTreePage{
		pageID:     pageID,
		pageType:   pageType,
		ParentPage: primitives.PageNumber(ParentPage),
		NextLeaf:   primitives.PageNumber(NextLeaf),
		PrevLeaf:   primitives.PageNumber(PrevLeaf),
		isDirty:    false,
	}

	// Deserialize Entries/InternalPages based on page type
	if pageType == pageTypeLeaf {
		btreePage.Entries = make([]*index.IndexEntry, 0, numEntries)
		for i := 0; i < int(numEntries); i++ {
			entry, err := deserializeEntry(buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize entry %d: %w", i, err)
			}
			btreePage.Entries = append(btreePage.Entries, entry)
			if i == 0 && entry.Key != nil {
				btreePage.keyType = entry.Key.Type()
			}
		}
	} else {
		btreePage.InternalPages = make([]*BTreeChildPtr, 0, numEntries+1)
		for i := 0; i <= int(numEntries); i++ {
			var key types.Field
			var err error

			// First child has no key
			if i > 0 {
				key, err = deserializeField(buf)
				if err != nil {
					return nil, fmt.Errorf("failed to deserialize key %d: %w", i, err)
				}
				if i == 1 && key != nil {
					btreePage.keyType = key.Type()
				}
			}

			var fileID, pageNum uint64
			binary.Read(buf, binary.BigEndian, &fileID)
			binary.Read(buf, binary.BigEndian, &pageNum)

			childPtr := &BTreeChildPtr{
				Key:      key,
				ChildPID: page.NewPageDescriptor(primitives.FileID(fileID), primitives.PageNumber(pageNum)),
			}
			btreePage.InternalPages = append(btreePage.InternalPages, childPtr)
		}
	}

	return btreePage, nil
}

// deserializeEntry reads an entry from the buffer
func deserializeEntry(r *bytes.Reader) (*index.IndexEntry, error) {
	key, err := deserializeField(r)
	if err != nil {
		return nil, err
	}

	// Read page ID type (kept for compatibility but not used anymore)
	var pageIDType byte
	binary.Read(r, binary.BigEndian, &pageIDType)

	var fileID, pageNum uint64
	var tupleNum uint16
	binary.Read(r, binary.BigEndian, &fileID)
	binary.Read(r, binary.BigEndian, &pageNum)
	binary.Read(r, binary.BigEndian, &tupleNum)

	pageID := page.NewPageDescriptor(primitives.FileID(fileID), primitives.PageNumber(pageNum))

	rid := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: primitives.SlotID(tupleNum),
	}

	return &index.IndexEntry{
		Key: key,
		RID: rid,
	}, nil
}

// deserializeField reads a field from the buffer
// This reads the field type byte first, then delegates to types.ParseField for consistency
func deserializeField(r *bytes.Reader) (types.Field, error) {
	fieldTypeByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	fieldType := types.Type(fieldTypeByte)
	return types.ParseField(r, fieldType)
}
