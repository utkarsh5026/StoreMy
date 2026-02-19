package index

import (
	"path/filepath"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// mockPage is a minimal page.Page implementation used to test WritePage error handling.
type mockPage struct{}

func (m mockPage) GetID() *page.PageDescriptor               { return nil }
func (m mockPage) IsDirty() *primitives.TransactionID        { return nil }
func (m mockPage) MarkDirty(bool, *primitives.TransactionID) {}
func (m mockPage) GetPageData() []byte                       { return nil }
func (m mockPage) GetBeforeImage() page.Page                 { return nil }
func (m mockPage) SetBeforeImage()                           {}

func TestNewBTreeLeafPage(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	page := NewBTreeLeafPage(pageID, types.IntType, primitives.InvalidPageNumber)

	if !page.IsLeafPage() {
		t.Error("Expected leaf page")
	}
	if page.IsInternalPage() {
		t.Error("Expected not internal page")
	}
	if page.GetNumEntries() != 0 {
		t.Errorf("Expected 0 entries, got %d", page.GetNumEntries())
	}
	if page.ParentPage != primitives.InvalidPageNumber {
		t.Errorf("Expected parent InvalidPageNumber, got %d", page.ParentPage)
	}
	if page.IsRoot() != true {
		t.Error("Expected page to be root")
	}
	if page.IsDirty() != nil {
		t.Error("Expected page to not be dirty")
	}
}

func TestNewBTreeInternalPage(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	page := NewBTreeInternalPage(pageID, types.IntType, primitives.InvalidPageNumber)

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
	if page.ParentPage != primitives.InvalidPageNumber {
		t.Errorf("Expected parent InvalidPageNumber, got %d", page.ParentPage)
	}
}

func TestLeafPageEntryOperations(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	btreePage := NewBTreeLeafPage(pageID, types.IntType, primitives.InvalidPageNumber)

	// Create test entries
	entry1 := &IndexEntry{
		Key: types.NewIntField(10),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 0,
		},
	}
	entry2 := &IndexEntry{
		Key: types.NewIntField(20),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 1,
		},
	}
	entry3 := &IndexEntry{
		Key: types.NewIntField(15),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 2,
		},
	}

	// Test insert at end
	if err := btreePage.InsertEntry(entry1, -1); err != nil {
		t.Fatalf("Failed to insert entry1: %v", err)
	}
	if btreePage.GetNumEntries() != 1 {
		t.Errorf("Expected 1 entry, got %d", btreePage.GetNumEntries())
	}

	// Test insert at specific position
	if err := btreePage.InsertEntry(entry2, 1); err != nil {
		t.Fatalf("Failed to insert entry2: %v", err)
	}
	if btreePage.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries, got %d", btreePage.GetNumEntries())
	}

	// Test insert in middle
	if err := btreePage.InsertEntry(entry3, 1); err != nil {
		t.Fatalf("Failed to insert entry3: %v", err)
	}
	if btreePage.GetNumEntries() != 3 {
		t.Errorf("Expected 3 entries, got %d", btreePage.GetNumEntries())
	}

	// Verify order: entry1(10), entry3(15), entry2(20)
	if eq, _ := btreePage.Entries[0].Key.Compare(primitives.Equals, entry1.Key); !eq {
		t.Error("Entry at position 0 incorrect")
	}
	if eq, _ := btreePage.Entries[1].Key.Compare(primitives.Equals, entry3.Key); !eq {
		t.Error("Entry at position 1 incorrect")
	}
	if eq, _ := btreePage.Entries[2].Key.Compare(primitives.Equals, entry2.Key); !eq {
		t.Error("Entry at position 2 incorrect")
	}

	// Test remove entry
	removed, err := btreePage.RemoveEntry(1)
	if err != nil {
		t.Fatalf("Failed to remove entry: %v", err)
	}
	if eq, _ := removed.Key.Compare(primitives.Equals, entry3.Key); !eq {
		t.Error("Removed wrong entry")
	}
	if btreePage.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries after removal, got %d", btreePage.GetNumEntries())
	}

	// Test remove last entry
	removed, err = btreePage.RemoveEntry(-1)
	if err != nil {
		t.Fatalf("Failed to remove last entry: %v", err)
	}
	if eq, _ := removed.Key.Compare(primitives.Equals, entry2.Key); !eq {
		t.Error("Removed wrong entry")
	}
	if btreePage.GetNumEntries() != 1 {
		t.Errorf("Expected 1 entry after removal, got %d", btreePage.GetNumEntries())
	}
}

func TestInternalPageChildOperations(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	btreePage := NewBTreeInternalPage(pageID, types.IntType, primitives.InvalidPageNumber)

	// Create child pointers
	child0 := NewBtreeChildPtr(nil, page.NewPageDescriptor(1, 1))
	child1 := NewBtreeChildPtr(types.NewIntField(10), page.NewPageDescriptor(1, 2))
	child2 := NewBtreeChildPtr(types.NewIntField(20), page.NewPageDescriptor(1, 3))

	// Add first child (no key)
	if err := btreePage.AddChildPtr(child0, 0); err != nil {
		t.Fatalf("Failed to add child0: %v", err)
	}
	if btreePage.GetNumEntries() != 0 {
		t.Errorf("Expected 0 entries with 1 child, got %d", btreePage.GetNumEntries())
	}

	// Add second child (with key)
	if err := btreePage.AddChildPtr(child1, 1); err != nil {
		t.Fatalf("Failed to add child1: %v", err)
	}
	if btreePage.GetNumEntries() != 1 {
		t.Errorf("Expected 1 entry with 2 children, got %d", btreePage.GetNumEntries())
	}

	// Add third child
	if err := btreePage.AddChildPtr(child2, 2); err != nil {
		t.Fatalf("Failed to add child2: %v", err)
	}
	if btreePage.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries with 3 children, got %d", btreePage.GetNumEntries())
	}

	// Test update child key
	if err := btreePage.UpdateChildrenKey(1, types.NewIntField(15)); err != nil {
		t.Fatalf("Failed to update child key: %v", err)
	}

	// Test remove child
	removed, err := btreePage.RemoveChildPtr(1)
	if err != nil {
		t.Fatalf("Failed to remove child: %v", err)
	}
	if eq, _ := removed.Key.Compare(primitives.Equals, types.NewIntField(15)); !eq {
		t.Error("Removed wrong child")
	}
	if btreePage.GetNumEntries() != 1 {
		t.Errorf("Expected 1 entry after removal, got %d", btreePage.GetNumEntries())
	}
}

func TestPageSerialization(t *testing.T) {
	// Create a leaf page with entries
	pageID := page.NewPageDescriptor(1, 5)
	btreePage := NewBTreeLeafPage(pageID, types.IntType, 2)
	btreePage.NextLeaf = 6
	btreePage.PrevLeaf = 4

	entry1 := &IndexEntry{
		Key: types.NewIntField(100),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 10),
			TupleNum: 5,
		},
	}
	entry2 := &IndexEntry{
		Key: types.NewIntField(200),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 11),
			TupleNum: 7,
		},
	}

	btreePage.InsertEntry(entry1, -1)
	btreePage.InsertEntry(entry2, -1)

	// Serialize
	data := btreePage.GetPageData()

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
	pageID := page.NewPageDescriptor(2, 3)
	btreePage := NewBTreeInternalPage(pageID, types.IntType, primitives.InvalidPageNumber)

	child0 := NewBtreeChildPtr(nil, page.NewPageDescriptor(2, 10))
	child1 := NewBtreeChildPtr(types.NewIntField(50), page.NewPageDescriptor(2, 11))
	child2 := NewBtreeChildPtr(types.NewIntField(100), page.NewPageDescriptor(2, 12))

	btreePage.AddChildPtr(child0, 0)
	btreePage.AddChildPtr(child1, 1)
	btreePage.AddChildPtr(child2, 2)

	// Serialize
	data := btreePage.GetPageData()

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
	pageID := page.NewPageDescriptor(1, 0)
	btreePage := NewBTreeLeafPage(pageID, types.IntType, primitives.InvalidPageNumber)

	// Initially not dirty
	if btreePage.IsDirty() != nil {
		t.Error("New page should not be dirty")
	}

	// Mark dirty
	txnID := primitives.NewTransactionIDFromValue(123)
	btreePage.MarkDirty(true, txnID)

	if btreePage.IsDirty() == nil {
		t.Error("Page should be dirty after marking")
	}
	if btreePage.IsDirty().ID() != 123 {
		t.Errorf("Expected txn ID %d, got %d", 123, btreePage.IsDirty().ID())
	}

	// Verify before image was captured
	if btreePage.beforeImage == nil {
		t.Error("Before image should be set when marking dirty")
	}

	// Add an entry to modify the page
	entry := &IndexEntry{
		Key: types.NewIntField(10),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 0,
		},
	}
	btreePage.InsertEntry(entry, -1)

	// Get before image
	beforePage := btreePage.GetBeforeImage()
	if beforePage == nil {
		t.Fatal("Before image should be retrievable")
	}

	beforeBTree := beforePage.(*BTreePage)
	if beforeBTree.GetNumEntries() != 0 {
		t.Error("Before image should have 0 entries")
	}
	if btreePage.GetNumEntries() != 1 {
		t.Error("Current page should have 1 entry")
	}
}

func TestPageCapacity(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	btreePage := NewBTreeLeafPage(pageID, types.IntType, primitives.InvalidPageNumber)

	if btreePage.IsFull() {
		t.Error("Empty page should not be full")
	}

	// Add entries up to max
	for i := 0; i < MaxEntriesPerPage; i++ {
		entry := &IndexEntry{
			Key: types.NewIntField(int64(i)),
			RID: &tuple.TupleRecordID{
				PageID:   page.NewPageDescriptor(1, 0),
				TupleNum: primitives.SlotID(i),
			},
		}
		btreePage.InsertEntry(entry, -1)
	}

	if !btreePage.IsFull() {
		t.Error("Page should be full after adding max entries")
	}

	if !btreePage.HasMoreThanRequired() {
		t.Error("Full page should have more than required entries")
	}
}

func TestPageRelations(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 5)
	btreePage := NewBTreeLeafPage(pageID, types.IntType, 2)

	// Test parent
	if btreePage.IsRoot() {
		t.Error("Page with parent should not be root")
	}
	if btreePage.ParentPage != 2 {
		t.Errorf("Expected parent 2, got %d", btreePage.ParentPage)
	}

	// Set new parent
	btreePage.ParentPage = 3
	if btreePage.ParentPage != 3 {
		t.Errorf("Expected parent 3, got %d", btreePage.ParentPage)
	}

	// Test leaf links
	if btreePage.NextLeaf != primitives.InvalidPageNumber {
		t.Error("Should not have next leaf initially")
	}

	btreePage.PrevLeaf = 4
	btreePage.NextLeaf = 6

	if btreePage.NextLeaf == primitives.InvalidPageNumber {
		t.Error("Should have next leaf")
	}

	left, right := btreePage.PrevLeaf, btreePage.NextLeaf
	if left != 4 || right != 6 {
		t.Errorf("Expected leaves (4, 6), got (%d, %d)", left, right)
	}
}

func TestPageWithStringKeys(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	btreePage := NewBTreeLeafPage(pageID, types.StringType, primitives.InvalidPageNumber)

	entry1 := &IndexEntry{
		Key: types.NewStringField("apple", types.StringMaxSize),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 0,
		},
	}
	entry2 := &IndexEntry{
		Key: types.NewStringField("banana", types.StringMaxSize),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 1,
		},
	}

	btreePage.InsertEntry(entry1, -1)
	btreePage.InsertEntry(entry2, -1)

	// Serialize and deserialize
	data := btreePage.GetPageData()
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
	pageID := page.NewPageDescriptor(1, 0)
	btreePage := NewBTreeLeafPage(pageID, types.IntType, primitives.InvalidPageNumber)

	// Test invalid insert index
	entry := &IndexEntry{
		Key: types.NewIntField(10),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 0,
		},
	}

	if err := btreePage.InsertEntry(entry, 10); err == nil {
		t.Error("Should error on invalid insert index")
	}
	if err := btreePage.InsertEntry(entry, -2); err == nil {
		t.Error("Should error on invalid negative index")
	}

	// Test invalid remove index
	btreePage.InsertEntry(entry, -1)
	if _, err := btreePage.RemoveEntry(10); err == nil {
		t.Error("Should error on invalid remove index")
	}
	if _, err := btreePage.RemoveEntry(-2); err == nil {
		t.Error("Should error on invalid negative remove index")
	}

	// Test internal page errors
	internalPage := NewBTreeInternalPage(page.NewPageDescriptor(1, 1), types.IntType, primitives.InvalidPageNumber)
	child := NewBtreeChildPtr(nil, page.NewPageDescriptor(1, 2))
	internalPage.AddChildPtr(child, 0)

	if err := internalPage.UpdateChildrenKey(0, types.NewIntField(10)); err == nil {
		t.Error("Should error when trying to update key of first child")
	}
}

// ---- BTreeFile tests ----

func newTestBTreeFile(t *testing.T, keyType types.Type) (*BTreeFile, primitives.Filepath) {
	t.Helper()
	dir := t.TempDir()
	path := primitives.Filepath(filepath.Join(dir, "btree.db"))
	bf, err := NewBTreeFile(path, keyType)
	if err != nil {
		t.Fatalf("NewBTreeFile: %v", err)
	}
	t.Cleanup(func() { bf.Close() })
	return bf, path
}

func TestNewBTreeFile(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	if bf.GetKeyType() != types.IntType {
		t.Errorf("expected IntType, got %v", bf.GetKeyType())
	}
	if bf.NumPages() != 0 {
		t.Errorf("expected 0 pages initially, got %d", bf.NumPages())
	}
}

func TestNewBTreeFileEmptyPath(t *testing.T) {
	_, err := NewBTreeFile("", types.IntType)
	if err == nil {
		t.Error("expected error for empty file path")
	}
}

func TestBTreeFileGetKeyType(t *testing.T) {
	intFile, _ := newTestBTreeFile(t, types.IntType)
	if intFile.GetKeyType() != types.IntType {
		t.Errorf("expected IntType, got %v", intFile.GetKeyType())
	}

	strFile, _ := newTestBTreeFile(t, types.StringType)
	if strFile.GetKeyType() != types.StringType {
		t.Errorf("expected StringType, got %v", strFile.GetKeyType())
	}
}

func TestBTreeFileGetTupleDesc(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	td := bf.GetTupleDesc()
	if td == nil {
		t.Fatal("GetTupleDesc returned nil")
	}
	if td.NumFields() != 1 {
		t.Errorf("expected 1 field, got %d", td.NumFields())
	}
}

func TestBTreeFileSetIndexIDAndGetID(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	// Before setting, GetID falls back to the BaseFile's hash-based ID.
	idBefore := bf.GetID()

	bf.SetIndexID(99)
	if bf.GetID() != 99 {
		t.Errorf("expected ID 99 after SetIndexID, got %d", bf.GetID())
	}
	if idBefore == 99 {
		// Highly unlikely but not a real failure; just note it.
		t.Log("hash-based ID happened to equal 99 — skipping before/after comparison")
	}
}

func TestBTreeFileWriteAndReadLeafPage(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	// bf.indexID == 0, so the page descriptor must also use fileID 0.
	pageID := page.NewPageDescriptor(0, 0)
	btreePage := NewBTreeLeafPage(pageID, types.IntType, primitives.InvalidPageNumber)

	entry := &IndexEntry{
		Key: types.NewIntField(42),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(0, 1),
			TupleNum: 3,
		},
	}
	if err := btreePage.InsertEntry(entry, -1); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}

	if err := bf.WriteBTreePage(btreePage); err != nil {
		t.Fatalf("WriteBTreePage: %v", err)
	}
	if bf.NumPages() != 1 {
		t.Errorf("expected 1 page after write, got %d", bf.NumPages())
	}

	readPage, err := bf.ReadBTreePage(pageID)
	if err != nil {
		t.Fatalf("ReadBTreePage: %v", err)
	}
	if !readPage.IsLeafPage() {
		t.Error("read page should be a leaf page")
	}
	if readPage.GetNumEntries() != 1 {
		t.Errorf("expected 1 entry, got %d", readPage.GetNumEntries())
	}
	if eq, _ := readPage.Entries[0].Key.Compare(primitives.Equals, types.NewIntField(42)); !eq {
		t.Error("entry key mismatch after read")
	}
	if readPage.Entries[0].RID.TupleNum != 3 {
		t.Errorf("expected TupleNum 3, got %d", readPage.Entries[0].RID.TupleNum)
	}
}

func TestBTreeFileWriteAndReadInternalPage(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	pageID := page.NewPageDescriptor(0, 0)
	btreePage := NewBTreeInternalPage(pageID, types.IntType, primitives.InvalidPageNumber)

	child0 := NewBtreeChildPtr(nil, page.NewPageDescriptor(0, 1))
	child1 := NewBtreeChildPtr(types.NewIntField(100), page.NewPageDescriptor(0, 2))
	btreePage.AddChildPtr(child0, 0)
	btreePage.AddChildPtr(child1, 1)

	if err := bf.WriteBTreePage(btreePage); err != nil {
		t.Fatalf("WriteBTreePage: %v", err)
	}

	readPage, err := bf.ReadBTreePage(pageID)
	if err != nil {
		t.Fatalf("ReadBTreePage: %v", err)
	}
	if !readPage.IsInternalPage() {
		t.Error("read page should be an internal page")
	}
	if readPage.GetNumEntries() != 1 {
		t.Errorf("expected 1 entry (2 children - 1), got %d", readPage.GetNumEntries())
	}
	if len(readPage.InternalPages) != 2 {
		t.Errorf("expected 2 children, got %d", len(readPage.InternalPages))
	}
	if eq, _ := readPage.InternalPages[1].Key.Compare(primitives.Equals, types.NewIntField(100)); !eq {
		t.Error("separator key mismatch after read")
	}
}

func TestBTreeFileNumPagesGrowth(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	if bf.NumPages() != 0 {
		t.Errorf("expected 0 pages initially, got %d", bf.NumPages())
	}

	for i := range 3 {
		p := NewBTreeLeafPage(page.NewPageDescriptor(0, primitives.PageNumber(i)), types.IntType, primitives.InvalidPageNumber)
		if err := bf.WriteBTreePage(p); err != nil {
			t.Fatalf("WriteBTreePage page %d: %v", i, err)
		}
	}

	if bf.NumPages() != 3 {
		t.Errorf("expected 3 pages after writes, got %d", bf.NumPages())
	}
}

func TestBTreeFileAllocatePage(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)
	txnID := primitives.NewTransactionIDFromValue(7)

	leafPage, err := bf.AllocatePage(txnID, types.IntType, true, primitives.InvalidPageNumber)
	if err != nil {
		t.Fatalf("AllocatePage (leaf): %v", err)
	}
	if !leafPage.IsLeafPage() {
		t.Error("allocated page should be a leaf")
	}
	if leafPage.IsDirty() == nil {
		t.Error("allocated page should be marked dirty")
	}
	if leafPage.IsDirty().ID() != 7 {
		t.Errorf("expected txn ID 7, got %d", leafPage.IsDirty().ID())
	}
	if leafPage.PageNo() != 0 {
		t.Errorf("expected page number 0, got %d", leafPage.PageNo())
	}

	internalPage, err := bf.AllocatePage(txnID, types.IntType, false, primitives.InvalidPageNumber)
	if err != nil {
		t.Fatalf("AllocatePage (internal): %v", err)
	}
	if !internalPage.IsInternalPage() {
		t.Error("allocated page should be an internal page")
	}
	if internalPage.PageNo() != 1 {
		t.Errorf("expected page number 1, got %d", internalPage.PageNo())
	}
	if bf.NumPages() != 2 {
		t.Errorf("expected 2 pages after allocations, got %d", bf.NumPages())
	}
}

func TestBTreeFileReadPageInterface(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	pageID := page.NewPageDescriptor(0, 0)
	if err := bf.WriteBTreePage(NewBTreeLeafPage(pageID, types.IntType, primitives.InvalidPageNumber)); err != nil {
		t.Fatalf("WriteBTreePage: %v", err)
	}

	p, err := bf.ReadPage(pageID)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if _, ok := p.(*BTreePage); !ok {
		t.Errorf("ReadPage should return *BTreePage, got %T", p)
	}
}

func TestBTreeFileWritePageInterface(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	pageID := page.NewPageDescriptor(0, 0)
	var p page.Page = NewBTreeLeafPage(pageID, types.IntType, primitives.InvalidPageNumber)
	if err := bf.WritePage(p); err != nil {
		t.Fatalf("WritePage: %v", err)
	}
	if bf.NumPages() != 1 {
		t.Errorf("expected 1 page after WritePage, got %d", bf.NumPages())
	}
}

func TestBTreeFileWritePageWrongType(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	if err := bf.WritePage(mockPage{}); err == nil {
		t.Error("expected error when writing a non-BTreePage")
	}
}

func TestBTreeFileReadPageErrors(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	// Nil page ID.
	if _, err := bf.ReadBTreePage(nil); err == nil {
		t.Error("expected error for nil page ID")
	}

	// FileID mismatch (bf.indexID == 0, but descriptor uses 5).
	if _, err := bf.ReadBTreePage(page.NewPageDescriptor(5, 0)); err == nil {
		t.Error("expected error for mismatched file ID")
	}

	// Page number beyond current file size.
	if _, err := bf.ReadBTreePage(page.NewPageDescriptor(0, 999)); err == nil {
		t.Error("expected error reading non-existent page")
	}
}

func TestBTreeFileWriteNilPage(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)

	if err := bf.WriteBTreePage(nil); err == nil {
		t.Error("expected error when writing nil page")
	}
}

func TestBTreeFileReadAfterSetIndexID(t *testing.T) {
	bf, _ := newTestBTreeFile(t, types.IntType)
	bf.SetIndexID(3)

	// Now the page descriptor must also carry fileID 3.
	pageID := page.NewPageDescriptor(3, 0)
	btreePage := NewBTreeLeafPage(pageID, types.IntType, primitives.InvalidPageNumber)
	if err := btreePage.InsertEntry(&IndexEntry{
		Key: types.NewIntField(77),
		RID: &tuple.TupleRecordID{PageID: page.NewPageDescriptor(3, 1), TupleNum: 0},
	}, -1); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}

	if err := bf.WriteBTreePage(btreePage); err != nil {
		t.Fatalf("WriteBTreePage: %v", err)
	}

	readPage, err := bf.ReadBTreePage(pageID)
	if err != nil {
		t.Fatalf("ReadBTreePage: %v", err)
	}
	if readPage.GetNumEntries() != 1 {
		t.Errorf("expected 1 entry, got %d", readPage.GetNumEntries())
	}
	if eq, _ := readPage.Entries[0].Key.Compare(primitives.Equals, types.NewIntField(77)); !eq {
		t.Error("key mismatch after SetIndexID round-trip")
	}
}
