package memory

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/log/wal"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
)

func pgNum(id int) primitives.PageNumber {
	return primitives.PageNumber(id)
}

// mockPage implements page.Page interface for testing
type mockPage struct {
	id        *page.PageDescriptor
	dirty     bool
	dirtyTid  *primitives.TransactionID
	data      []byte
	beforeImg page.Page
	mutex     sync.RWMutex
}

func newMockPage(pageID *page.PageDescriptor) *mockPage {
	return &mockPage{
		id:   pageID,
		data: make([]byte, page.PageSize),
	}
}

func (m *mockPage) GetID() *page.PageDescriptor {
	return m.id
}

func (m *mockPage) IsDirty() *primitives.TransactionID {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if m.dirty {
		return m.dirtyTid
	}
	return nil
}

func (m *mockPage) MarkDirty(dirty bool, tid *primitives.TransactionID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.dirty = dirty
	if dirty {
		m.dirtyTid = tid
	} else {
		m.dirtyTid = nil
	}
}

func (m *mockPage) GetPageData() []byte {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	dataCopy := make([]byte, len(m.data))
	copy(dataCopy, m.data)
	return dataCopy
}

func (m *mockPage) GetBeforeImage() page.Page {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.beforeImg
}

func (m *mockPage) SetBeforeImage() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	beforeData := make([]byte, len(m.data))
	copy(beforeData, m.data)
	m.beforeImg = &mockPage{
		id:   m.id,
		data: beforeData,
	}
}

// mockDbFileForPageStore implements page.DbFile for PageStore testing
type mockDbFileForPageStore struct {
	id        int
	tupleDesc *tuple.TupleDescription
	pages     map[primitives.PageID]*mockPage
	mutex     sync.RWMutex
}

func newMockDbFileForPageStore(id int, fieldTypes []types.Type, fieldNames []string) *mockDbFileForPageStore {
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		panic("Failed to create TupleDescription: " + err.Error())
	}
	return &mockDbFileForPageStore{
		id:        id,
		tupleDesc: td,
		pages:     make(map[primitives.PageID]*mockPage),
	}
}

func (m *mockDbFileForPageStore) ReadPage(pid *page.PageDescriptor) (page.Page, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if p, exists := m.pages[pid]; exists {
		return p, nil
	}

	// Create new page if it doesn't exist
	newPage := newMockPage(pid)
	m.pages[pid] = newPage
	return newPage, nil
}

func (m *mockDbFileForPageStore) WritePage(p page.Page) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	mockP, ok := p.(*mockPage)
	if !ok {
		return fmt.Errorf("expected mockPage, got %T", p)
	}

	m.pages[p.GetID()] = mockP
	return nil
}

func (m *mockDbFileForPageStore) AddTuple(tid *primitives.TransactionID, t *tuple.Tuple) ([]page.Page, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Create a new page for the tuple
	pageID := page.NewPageDescriptor(primitives.FileID(m.id), primitives.PageNumber(len(m.pages)))
	newPage := newMockPage(pageID)
	newPage.MarkDirty(true, tid)

	// Set the RecordID on the tuple so tests can use it (if tuple is not nil)
	if t != nil {
		t.RecordID = tuple.NewTupleRecordID(pageID, 0) // Mock assigns tuple to slot 0
	}

	m.pages[pageID] = newPage
	return []page.Page{newPage}, nil
}

func (m *mockDbFileForPageStore) DeleteTuple(tid *primitives.TransactionID, t *tuple.Tuple) (page.Page, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Create or return a page for the delete operation
	if t.RecordID != nil {
		pageID := t.RecordID.PageID
		if existingPage, exists := m.pages[pageID]; exists {
			existingPage.MarkDirty(true, tid)
			return existingPage, nil
		}

		hpid := pageID.(*page.PageDescriptor)
		newPage := newMockPage(hpid)
		newPage.MarkDirty(true, tid)
		m.pages[pageID] = newPage
		return newPage, nil
	}

	return nil, fmt.Errorf("tuple has no record ID")
}

func (m *mockDbFileForPageStore) Iterator(tid *primitives.TransactionID) iterator.DbFileIterator {
	return nil
}

func (m *mockDbFileForPageStore) GetID() int {
	return m.id
}

func (m *mockDbFileForPageStore) GetTupleDesc() *tuple.TupleDescription {
	return m.tupleDesc
}

func (m *mockDbFileForPageStore) Close() error {
	return nil
}

func (m *mockDbFileForPageStore) IsClosed() bool {
	return false
}

func createTransactionContext(t *testing.T, wal *wal.WAL) *transaction.TransactionContext {
	t.Helper()

	rg := transaction.NewTransactionRegistry(wal)
	ctx, err := rg.Begin()

	if err != nil {
		t.Fatalf("Error creating transaction Context")
	}

	return ctx
}
