package btree

import (
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

func TestNewBTreeLeafPage(t *testing.T) {
	pageID := NewBTreePageID(1, 0)
	page := NewBTreeLeafPage(pageID, types.IntType, NoPage)

	if !page.IsLeafPage() {
		t.Error("Expected leaf page")
	}
	if page.IsInternalPage() {
		t.Error("Expected not internal page")
	}
	if page.GetNumEntries() != 0 {
		t.Errorf("Expected 0 entries, got %d", page.GetNumEntries())
	}
	if page.ParentPage != NoPage {
		t.Errorf("Expected parent NoPage, got %d", page.ParentPage)
	}
	if page.IsRoot() != true {
		t.Error("Expected page to be root")
	}
	if page.IsDirty() != nil {
		t.Error("Expected page to not be dirty")
	}
}

func TestNewBTreeInternalPage(t *testing.T) {
	pageID := NewBTreePageID(1, 0)
	page := NewBTreeInternalPage(pageID, types.IntType, NoPage)

	if page.IsLeafPage() {
		t.Error("Expected not leaf page")
	}
	if !page.IsInternalPage() {
		t.Error("Expected internal page")
	}
	// Internal page with no children has -1 entries (len(InternalPages) - 1 = 0 - 1)
	if page.GetNumEntries() != -1 {
		t.Errorf("Expected -1 entries for empty internal page, got %d", page.GetNumEntries())
	}
	if page.ParentPage != NoPage {
		t.Errorf("Expected parent NoPage, got %d", page.ParentPage)
	}
}

func TestLeafPageEntryOperations(t *testing.T) {
	pageID := NewBTreePageID(1, 0)
	page := NewBTreeLeafPage(pageID, types.IntType, NoPage)

	// Create test entries
	entry1 := &index.IndexEntry{
		Key: types.NewIntField(10),
		RID: &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, 0),
			TupleNum: 0,
		},
	}
	entry2 := &index.IndexEntry{
		Key: types.NewIntField(20),
		RID: &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, 0),
			TupleNum: 1,
		},
	}
	entry3 := &index.IndexEntry{
		Key: types.NewIntField(15),
		RID: &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, 0),
			TupleNum: 2,
		},
	}

	// Test insert at end
	if err := page.InsertEntry(entry1, -1); err != nil {
		t.Fatalf("Failed to insert entry1: %v", err)
	}
	if page.GetNumEntries() != 1 {
		t.Errorf("Expected 1 entry, got %d", page.GetNumEntries())
	}

	// Test insert at specific position
	if err := page.InsertEntry(entry2, 1); err != nil {
		t.Fatalf("Failed to insert entry2: %v", err)
	}
	if page.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries, got %d", page.GetNumEntries())
	}

	// Test insert in middle
	if err := page.InsertEntry(entry3, 1); err != nil {
		t.Fatalf("Failed to insert entry3: %v", err)
	}
	if page.GetNumEntries() != 3 {
		t.Errorf("Expected 3 entries, got %d", page.GetNumEntries())
	}

	// Verify order: entry1(10), entry3(15), entry2(20)
	if eq, _ := page.Entries[0].Key.Compare(primitives.Equals, entry1.Key); !eq {
		t.Error("Entry at position 0 incorrect")
	}
	if eq, _ := page.Entries[1].Key.Compare(primitives.Equals, entry3.Key); !eq {
		t.Error("Entry at position 1 incorrect")
	}
	if eq, _ := page.Entries[2].Key.Compare(primitives.Equals, entry2.Key); !eq {
		t.Error("Entry at position 2 incorrect")
	}

	// Test remove entry
	removed, err := page.RemoveEntry(1)
	if err != nil {
		t.Fatalf("Failed to remove entry: %v", err)
	}
	if eq, _ := removed.Key.Compare(primitives.Equals, entry3.Key); !eq {
		t.Error("Removed wrong entry")
	}
	if page.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries after removal, got %d", page.GetNumEntries())
	}

	// Test remove last entry
	removed, err = page.RemoveEntry(-1)
	if err != nil {
		t.Fatalf("Failed to remove last entry: %v", err)
	}
	if eq, _ := removed.Key.Compare(primitives.Equals, entry2.Key); !eq {
		t.Error("Removed wrong entry")
	}
	if page.GetNumEntries() != 1 {
		t.Errorf("Expected 1 entry after removal, got %d", page.GetNumEntries())
	}
}

func TestInternalPageChildOperations(t *testing.T) {
	pageID := NewBTreePageID(1, 0)
	page := NewBTreeInternalPage(pageID, types.IntType, NoPage)

	// Create child pointers
	child0 := NewBtreeChildPtr(nil, NewBTreePageID(1, 1))
	child1 := NewBtreeChildPtr(types.NewIntField(10), NewBTreePageID(1, 2))
	child2 := NewBtreeChildPtr(types.NewIntField(20), NewBTreePageID(1, 3))

	// Add first child (no key)
	if err := page.AddChildPtr(child0, 0); err != nil {
		t.Fatalf("Failed to add child0: %v", err)
	}
	if page.GetNumEntries() != 0 {
		t.Errorf("Expected 0 entries with 1 child, got %d", page.GetNumEntries())
	}

	// Add second child (with key)
	if err := page.AddChildPtr(child1, 1); err != nil {
		t.Fatalf("Failed to add child1: %v", err)
	}
	if page.GetNumEntries() != 1 {
		t.Errorf("Expected 1 entry with 2 children, got %d", page.GetNumEntries())
	}

	// Add third child
	if err := page.AddChildPtr(child2, 2); err != nil {
		t.Fatalf("Failed to add child2: %v", err)
	}
	if page.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries with 3 children, got %d", page.GetNumEntries())
	}

	// Test get child key
	key, err := page.GetChildKey(1)
	if err != nil {
		t.Fatalf("Failed to get child key: %v", err)
	}
	if eq, _ := key.Compare(primitives.Equals, types.NewIntField(10)); !eq {
		t.Error("Got wrong child key")
	}

	// Test update child key
	if err := page.UpdateChildrenKey(1, types.NewIntField(15)); err != nil {
		t.Fatalf("Failed to update child key: %v", err)
	}
	key, _ = page.GetChildKey(1)
	if eq, _ := key.Compare(primitives.Equals, types.NewIntField(15)); !eq {
		t.Error("Child key not updated correctly")
	}

	// Test remove child
	removed, err := page.RemoveChildPtr(1)
	if err != nil {
		t.Fatalf("Failed to remove child: %v", err)
	}
	if eq, _ := removed.Key.Compare(primitives.Equals, types.NewIntField(15)); !eq {
		t.Error("Removed wrong child")
	}
	if page.GetNumEntries() != 1 {
		t.Errorf("Expected 1 entry after removal, got %d", page.GetNumEntries())
	}
}

func TestPageSerialization(t *testing.T) {
	// Create a leaf page with entries
	pageID := NewBTreePageID(1, 5)
	page := NewBTreeLeafPage(pageID, types.IntType, 2)
	page.NextLeaf = 6
	page.PrevLeaf = 4

	entry1 := &index.IndexEntry{
		Key: types.NewIntField(100),
		RID: &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, 10),
			TupleNum: 5,
		},
	}
	entry2 := &index.IndexEntry{
		Key: types.NewIntField(200),
		RID: &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, 11),
			TupleNum: 7,
		},
	}

	page.InsertEntry(entry1, -1)
	page.InsertEntry(entry2, -1)

	// Serialize
	data := page.GetPageData()

	// Deserialize
	deserializedPage, err := DeserializeBTreePage(data, pageID)
	if err != nil {
		t.Fatalf("Failed to deserialize page: %v", err)
	}

	// Verify page properties
	if !deserializedPage.IsLeafPage() {
		t.Error("Deserialized page should be leaf")
	}
	if deserializedPage.ParentPage != 2 {
		t.Errorf("Expected parent 2, got %d", deserializedPage.ParentPage)
	}
	if deserializedPage.NextLeaf != 6 {
		t.Errorf("Expected NextLeaf 6, got %d", deserializedPage.NextLeaf)
	}
	if deserializedPage.PrevLeaf != 4 {
		t.Errorf("Expected PrevLeaf 4, got %d", deserializedPage.PrevLeaf)
	}
	if deserializedPage.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries, got %d", deserializedPage.GetNumEntries())
	}

	// Verify entries
	if eq, _ := deserializedPage.Entries[0].Key.Compare(primitives.Equals, entry1.Key); !eq {
		t.Error("First entry key mismatch")
	}
	if deserializedPage.Entries[0].RID.TupleNum != 5 {
		t.Error("First entry RID mismatch")
	}
	if eq, _ := deserializedPage.Entries[1].Key.Compare(primitives.Equals, entry2.Key); !eq {
		t.Error("Second entry key mismatch")
	}
}

func TestInternalPageSerialization(t *testing.T) {
	pageID := NewBTreePageID(2, 3)
	page := NewBTreeInternalPage(pageID, types.IntType, NoPage)

	child0 := NewBtreeChildPtr(nil, NewBTreePageID(2, 10))
	child1 := NewBtreeChildPtr(types.NewIntField(50), NewBTreePageID(2, 11))
	child2 := NewBtreeChildPtr(types.NewIntField(100), NewBTreePageID(2, 12))

	page.AddChildPtr(child0, 0)
	page.AddChildPtr(child1, 1)
	page.AddChildPtr(child2, 2)

	// Serialize
	data := page.GetPageData()

	// Deserialize
	deserializedPage, err := DeserializeBTreePage(data, pageID)
	if err != nil {
		t.Fatalf("Failed to deserialize internal page: %v", err)
	}

	// Verify
	if !deserializedPage.IsInternalPage() {
		t.Error("Deserialized page should be internal")
	}
	if deserializedPage.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries, got %d", deserializedPage.GetNumEntries())
	}
	if len(deserializedPage.InternalPages) != 3 {
		t.Errorf("Expected 3 children, got %d", len(deserializedPage.InternalPages))
	}

	// Verify first child has no key
	if deserializedPage.InternalPages[0].Key != nil {
		t.Error("First child should have no key")
	}
	if deserializedPage.InternalPages[0].ChildPID.PageNo() != 10 {
		t.Error("First child PID mismatch")
	}

	// Verify second child
	if eq, _ := deserializedPage.InternalPages[1].Key.Compare(primitives.Equals, types.NewIntField(50)); !eq {
		t.Error("Second child key mismatch")
	}

	// Verify third child
	if eq, _ := deserializedPage.InternalPages[2].Key.Compare(primitives.Equals, types.NewIntField(100)); !eq {
		t.Error("Third child key mismatch")
	}
}

func TestDirtyTracking(t *testing.T) {
	pageID := NewBTreePageID(1, 0)
	page := NewBTreeLeafPage(pageID, types.IntType, NoPage)

	// Initially not dirty
	if page.IsDirty() != nil {
		t.Error("New page should not be dirty")
	}

	// Mark dirty
	txnID := primitives.NewTransactionIDFromValue(123)
	page.MarkDirty(true, txnID)

	if page.IsDirty() == nil {
		t.Error("Page should be dirty after marking")
	}
	if page.IsDirty().ID() != 123 {
		t.Errorf("Expected txn ID %d, got %d", 123, page.IsDirty().ID())
	}

	// Verify before image was captured
	if page.beforeImage == nil {
		t.Error("Before image should be set when marking dirty")
	}

	// Add an entry to modify the page
	entry := &index.IndexEntry{
		Key: types.NewIntField(10),
		RID: &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, 0),
			TupleNum: 0,
		},
	}
	page.InsertEntry(entry, -1)

	// Get before image
	beforePage := page.GetBeforeImage()
	if beforePage == nil {
		t.Fatal("Before image should be retrievable")
	}

	beforeBTree := beforePage.(*BTreePage)
	if beforeBTree.GetNumEntries() != 0 {
		t.Error("Before image should have 0 entries")
	}
	if page.GetNumEntries() != 1 {
		t.Error("Current page should have 1 entry")
	}
}

func TestPageCapacity(t *testing.T) {
	pageID := NewBTreePageID(1, 0)
	page := NewBTreeLeafPage(pageID, types.IntType, NoPage)

	if page.IsFull() {
		t.Error("Empty page should not be full")
	}

	// Add entries up to max
	for i := 0; i < MaxEntriesPerPage; i++ {
		entry := &index.IndexEntry{
			Key: types.NewIntField(int64(i)),
			RID: &tuple.TupleRecordID{
				PageID:   heap.NewHeapPageID(1, 0),
				TupleNum: i,
			},
		}
		page.InsertEntry(entry, -1)
	}

	if !page.IsFull() {
		t.Error("Page should be full after adding max entries")
	}

	if !page.HasMoreThanRequired() {
		t.Error("Full page should have more than required entries")
	}
}

func TestPageRelations(t *testing.T) {
	pageID := NewBTreePageID(1, 5)
	page := NewBTreeLeafPage(pageID, types.IntType, 2)

	// Test parent
	if page.IsRoot() {
		t.Error("Page with parent should not be root")
	}
	if page.Parent() != 2 {
		t.Errorf("Expected parent 2, got %d", page.Parent())
	}

	// Set new parent
	page.SetParent(3)
	if page.Parent() != 3 {
		t.Errorf("Expected parent 3, got %d", page.Parent())
	}

	// Test leaf links
	if page.HasPreviousLeaf() {
		t.Error("Should not have previous leaf initially")
	}
	if page.HasNextLeaf() {
		t.Error("Should not have next leaf initially")
	}

	page.PrevLeaf = 4
	page.NextLeaf = 6

	if !page.HasPreviousLeaf() {
		t.Error("Should have previous leaf")
	}
	if !page.HasNextLeaf() {
		t.Error("Should have next leaf")
	}

	left, right := page.Leaves()
	if left != 4 || right != 6 {
		t.Errorf("Expected leaves (4, 6), got (%d, %d)", left, right)
	}
}

func TestPageWithStringKeys(t *testing.T) {
	pageID := NewBTreePageID(1, 0)
	page := NewBTreeLeafPage(pageID, types.StringType, NoPage)

	entry1 := &index.IndexEntry{
		Key: types.NewStringField("apple", types.StringMaxSize),
		RID: &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, 0),
			TupleNum: 0,
		},
	}
	entry2 := &index.IndexEntry{
		Key: types.NewStringField("banana", types.StringMaxSize),
		RID: &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, 0),
			TupleNum: 1,
		},
	}

	page.InsertEntry(entry1, -1)
	page.InsertEntry(entry2, -1)

	// Serialize and deserialize
	data := page.GetPageData()
	deserializedPage, err := DeserializeBTreePage(data, pageID)
	if err != nil {
		t.Fatalf("Failed to deserialize page with string keys: %v", err)
	}

	if deserializedPage.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries, got %d", deserializedPage.GetNumEntries())
	}

	if eq, _ := deserializedPage.Entries[0].Key.Compare(primitives.Equals, entry1.Key); !eq {
		t.Error("String key mismatch for first entry")
	}
	if eq, _ := deserializedPage.Entries[1].Key.Compare(primitives.Equals, entry2.Key); !eq {
		t.Error("String key mismatch for second entry")
	}
}

func TestErrorConditions(t *testing.T) {
	pageID := NewBTreePageID(1, 0)
	page := NewBTreeLeafPage(pageID, types.IntType, NoPage)

	// Test invalid insert index
	entry := &index.IndexEntry{
		Key: types.NewIntField(10),
		RID: &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, 0),
			TupleNum: 0,
		},
	}

	if err := page.InsertEntry(entry, 10); err == nil {
		t.Error("Should error on invalid insert index")
	}
	if err := page.InsertEntry(entry, -2); err == nil {
		t.Error("Should error on invalid negative index")
	}

	// Test invalid remove index
	page.InsertEntry(entry, -1)
	if _, err := page.RemoveEntry(10); err == nil {
		t.Error("Should error on invalid remove index")
	}
	if _, err := page.RemoveEntry(-2); err == nil {
		t.Error("Should error on invalid negative remove index")
	}

	// Test internal page errors
	internalPage := NewBTreeInternalPage(NewBTreePageID(1, 1), types.IntType, NoPage)
	child := NewBtreeChildPtr(nil, NewBTreePageID(1, 2))
	internalPage.AddChildPtr(child, 0)

	if err := internalPage.UpdateChildrenKey(0, types.NewIntField(10)); err == nil {
		t.Error("Should error when trying to update key of first child")
	}
	if _, err := internalPage.GetChildKey(10); err == nil {
		t.Error("Should error on invalid child index")
	}
}
