package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
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

	var ParentPage, numEntries, NextLeaf, PrevLeaf int32
	binary.Read(buf, binary.BigEndian, &ParentPage)
	binary.Read(buf, binary.BigEndian, &numEntries)
	binary.Read(buf, binary.BigEndian, &NextLeaf)
	binary.Read(buf, binary.BigEndian, &PrevLeaf)

	page := &BTreePage{
		pageID:     pageID,
		pageType:   pageType,
		ParentPage: int(ParentPage),
		NextLeaf:   int(NextLeaf),
		PrevLeaf:   int(PrevLeaf),
		isDirty:    false,
	}

	// Deserialize Entries/InternalPages based on page type
	if pageType == pageTypeLeaf {
		page.Entries = make([]*index.IndexEntry, 0, numEntries)
		for i := 0; i < int(numEntries); i++ {
			entry, err := deserializeEntry(buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize entry %d: %w", i, err)
			}
			page.Entries = append(page.Entries, entry)
			if i == 0 && entry.Key != nil {
				page.keyType = entry.Key.Type()
			}
		}
	} else {
		page.InternalPages = make([]*BTreeChildPtr, 0, numEntries+1)
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
					page.keyType = key.Type()
				}
			}

			var tableID, pageNum int32
			binary.Read(buf, binary.BigEndian, &tableID)
			binary.Read(buf, binary.BigEndian, &pageNum)

			childPtr := &BTreeChildPtr{
				Key:      key,
				ChildPID: NewBTreePageID(int(tableID), int(pageNum)),
			}
			page.InternalPages = append(page.InternalPages, childPtr)
		}
	}

	return page, nil
}

// deserializeEntry reads an entry from the buffer
func deserializeEntry(r *bytes.Reader) (*index.IndexEntry, error) {
	key, err := deserializeField(r)
	if err != nil {
		return nil, err
	}

	// Read page ID type
	var pageIDType byte
	binary.Read(r, binary.BigEndian, &pageIDType)

	var tableID, pageNum, tupleNum int32
	binary.Read(r, binary.BigEndian, &tableID)
	binary.Read(r, binary.BigEndian, &pageNum)
	binary.Read(r, binary.BigEndian, &tupleNum)

	// Create the correct PageID type
	var pageID primitives.PageID
	if pageIDType == 1 {
		pageID = NewBTreePageID(int(tableID), int(pageNum))
	} else {
		pageID = heap.NewHeapPageID(int(tableID), int(pageNum))
	}

	rid := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: int(tupleNum),
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
