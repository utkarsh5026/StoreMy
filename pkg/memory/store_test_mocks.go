package memory

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// mockPage implements page.Page interface for testing
type mockPage struct {
	id        tuple.PageID
	dirty     bool
	dirtyTid  *transaction.TransactionID
	data      []byte
	beforeImg page.Page
	mutex     sync.RWMutex
}

func newMockPage(pageID tuple.PageID) *mockPage {
	return &mockPage{
		id:   pageID,
		data: make([]byte, page.PageSize),
	}
}

func (m *mockPage) GetID() tuple.PageID {
	return m.id
}

func (m *mockPage) IsDirty() *transaction.TransactionID {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if m.dirty {
		return m.dirtyTid
	}
	return nil
}

func (m *mockPage) MarkDirty(dirty bool, tid *transaction.TransactionID) {
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
	pages     map[tuple.PageID]*mockPage
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
		pages:     make(map[tuple.PageID]*mockPage),
	}
}

func (m *mockDbFileForPageStore) ReadPage(pid tuple.PageID) (page.Page, error) {
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

func (m *mockDbFileForPageStore) AddTuple(tid *transaction.TransactionID, t *tuple.Tuple) ([]page.Page, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Create a new page for the tuple
	pageID := heap.NewHeapPageID(m.id, len(m.pages))
	newPage := newMockPage(pageID)
	newPage.MarkDirty(true, tid)

	m.pages[pageID] = newPage
	return []page.Page{newPage}, nil
}

func (m *mockDbFileForPageStore) DeleteTuple(tid *transaction.TransactionID, t *tuple.Tuple) (page.Page, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Create or return a page for the delete operation
	if t.RecordID != nil {
		pageID := t.RecordID.PageID
		if existingPage, exists := m.pages[pageID]; exists {
			existingPage.MarkDirty(true, tid)
			return existingPage, nil
		}

		// Create new page if it doesn't exist
		newPage := newMockPage(pageID)
		newPage.MarkDirty(true, tid)
		m.pages[pageID] = newPage
		return newPage, nil
	}

	return nil, fmt.Errorf("tuple has no record ID")
}

func (m *mockDbFileForPageStore) Iterator(tid *transaction.TransactionID) iterator.DbFileIterator {
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
