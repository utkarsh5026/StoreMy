package memory

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"sync"
	"testing"
)

// Tests

func TestNewLRUPageCache(t *testing.T) {
	cache := NewLRUPageCache(10)

	if cache == nil {
		t.Fatal("NewLRUPageCache returned nil")
	}

	if cache.maxSize != 10 {
		t.Errorf("Expected maxSize=10, got %d", cache.maxSize)
	}

	if cache.Size() != 0 {
		t.Errorf("Expected initial size=0, got %d", cache.Size())
	}

	if cache.cache == nil {
		t.Error("Cache map not initialized")
	}

	if cache.lru == nil {
		t.Error("LRU list not initialized")
	}
}

func TestLRUPageCache_Put_Get(t *testing.T) {
	cache := NewLRUPageCache(5)

	pid1 := heap.NewHeapPageID(1, 1)
	page1 := newMockPage(pid1)

	// Put a page
	err := cache.Put(pid1, page1)
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	if cache.Size() != 1 {
		t.Errorf("Expected size=1, got %d", cache.Size())
	}

	// Get the page
	retrievedPage, found := cache.Get(pid1)
	if !found {
		t.Error("Page not found after Put")
	}

	if retrievedPage != page1 {
		t.Error("Retrieved page doesn't match put page")
	}
}

func TestLRUPageCache_Get_NonExistent(t *testing.T) {
	cache := NewLRUPageCache(5)

	pid := heap.NewHeapPageID(1, 1)
	_, found := cache.Get(pid)

	if found {
		t.Error("Expected not found for non-existent page")
	}
}

func TestLRUPageCache_Put_Update(t *testing.T) {
	cache := NewLRUPageCache(5)

	pid := heap.NewHeapPageID(1, 1)
	page1 := newMockPage(pid)
	page2 := newMockPage(pid)

	// Put initial page
	cache.Put(pid, page1)

	// Update with new page
	err := cache.Put(pid, page2)
	if err != nil {
		t.Errorf("Update failed: %v", err)
	}

	if cache.Size() != 1 {
		t.Errorf("Expected size=1 after update, got %d", cache.Size())
	}

	// Verify updated page
	retrievedPage, _ := cache.Get(pid)
	if retrievedPage != page2 {
		t.Error("Page was not updated")
	}
}

func TestLRUPageCache_Put_CacheFull(t *testing.T) {
	cache := NewLRUPageCache(3)

	// Fill the cache
	for i := 1; i <= 3; i++ {
		pid := heap.NewHeapPageID(1, i)
		p := newMockPage(pid)
		err := cache.Put(pid, p)
		if err != nil {
			t.Errorf("Put failed for page %d: %v", i, err)
		}
	}

	if cache.Size() != 3 {
		t.Errorf("Expected size=3, got %d", cache.Size())
	}

	// Try to add one more page
	pid4 := heap.NewHeapPageID(1, 4)
	page4 := newMockPage(pid4)
	err := cache.Put(pid4, page4)

	if err == nil {
		t.Error("Expected error when cache is full, got nil")
	}

	if cache.Size() != 3 {
		t.Errorf("Expected size to remain 3, got %d", cache.Size())
	}
}

func TestLRUPageCache_Remove(t *testing.T) {
	cache := NewLRUPageCache(5)

	pid1 := heap.NewHeapPageID(1, 1)
	page1 := newMockPage(pid1)

	// Put and then remove
	cache.Put(pid1, page1)
	cache.Remove(pid1)

	if cache.Size() != 0 {
		t.Errorf("Expected size=0 after remove, got %d", cache.Size())
	}

	_, found := cache.Get(pid1)
	if found {
		t.Error("Page should not be found after removal")
	}
}

func TestLRUPageCache_Remove_NonExistent(t *testing.T) {
	cache := NewLRUPageCache(5)

	pid := heap.NewHeapPageID(1, 1)

	// Should not panic
	cache.Remove(pid)

	if cache.Size() != 0 {
		t.Errorf("Expected size=0, got %d", cache.Size())
	}
}

func TestLRUPageCache_Clear(t *testing.T) {
	cache := NewLRUPageCache(5)

	// Store page IDs so we can reuse them
	pids := make([]primitives.PageID, 3)

	// Add multiple pages
	for i := 1; i <= 3; i++ {
		pid := heap.NewHeapPageID(1, i)
		pids[i-1] = pid
		p := newMockPage(pid)
		cache.Put(pid, p)
	}

	if cache.Size() != 3 {
		t.Errorf("Expected size=3 before clear, got %d", cache.Size())
	}

	// Clear the cache
	cache.Clear()

	if cache.Size() != 0 {
		t.Errorf("Expected size=0 after clear, got %d", cache.Size())
	}

	// Verify pages are actually removed
	for i := 0; i < 3; i++ {
		_, found := cache.Get(pids[i])
		if found {
			t.Errorf("Page %d should not be found after clear", i+1)
		}
	}
}

func TestLRUPageCache_Size(t *testing.T) {
	cache := NewLRUPageCache(5)

	if cache.Size() != 0 {
		t.Errorf("Expected initial size=0, got %d", cache.Size())
	}

	// Store page IDs so we can reuse them
	pids := make([]primitives.PageID, 3)

	// Add pages and verify size
	for i := 1; i <= 3; i++ {
		pid := heap.NewHeapPageID(1, i)
		pids[i-1] = pid
		p := newMockPage(pid)
		cache.Put(pid, p)

		if cache.Size() != i {
			t.Errorf("Expected size=%d, got %d", i, cache.Size())
		}
	}

	// Remove a page and verify size
	cache.Remove(pids[1]) // Remove the second page (1, 2)
	if cache.Size() != 2 {
		t.Errorf("Expected size=2 after remove, got %d", cache.Size())
	}
}

func TestLRUPageCache_GetAll(t *testing.T) {
	cache := NewLRUPageCache(5)

	// Empty cache
	pids := cache.GetAll()
	if len(pids) != 0 {
		t.Errorf("Expected 0 page IDs, got %d", len(pids))
	}

	// Add pages
	pid1 := heap.NewHeapPageID(1, 1)
	pid2 := heap.NewHeapPageID(1, 2)
	pid3 := heap.NewHeapPageID(1, 3)

	cache.Put(pid1, newMockPage(pid1))
	cache.Put(pid2, newMockPage(pid2))
	cache.Put(pid3, newMockPage(pid3))

	pids = cache.GetAll()
	if len(pids) != 3 {
		t.Errorf("Expected 3 page IDs, got %d", len(pids))
	}

	// Verify all pages are present
	pidMap := make(map[int]bool)
	for _, pid := range pids {
		pidMap[pid.PageNo()] = true
	}

	for i := 1; i <= 3; i++ {
		if !pidMap[i] {
			t.Errorf("Page %d not found in GetAll result", i)
		}
	}
}

func TestLRUPageCache_GetAll_LRUOrder(t *testing.T) {
	cache := NewLRUPageCache(5)

	// Add pages in order
	pid1 := heap.NewHeapPageID(1, 1)
	pid2 := heap.NewHeapPageID(1, 2)
	pid3 := heap.NewHeapPageID(1, 3)

	cache.Put(pid1, newMockPage(pid1))
	cache.Put(pid2, newMockPage(pid2))
	cache.Put(pid3, newMockPage(pid3))

	// Access pid1 to make it most recently used
	cache.Get(pid1)

	// GetAll should return in LRU order (least recently used first)
	// Expected order: pid2 (LRU), pid3, pid1 (MRU)
	pids := cache.GetAll()

	if len(pids) != 3 {
		t.Fatalf("Expected 3 page IDs, got %d", len(pids))
	}

	// pid2 should be first (least recently used)
	if !pids[0].Equals(pid2) {
		t.Errorf("Expected first element to be pid2, got %v", pids[0])
	}

	// pid1 should be last (most recently used)
	if !pids[2].Equals(pid1) {
		t.Errorf("Expected last element to be pid1, got %v", pids[2])
	}
}

func TestLRUPageCache_LRUBehavior(t *testing.T) {
	cache := NewLRUPageCache(3)

	pid1 := heap.NewHeapPageID(1, 1)
	pid2 := heap.NewHeapPageID(1, 2)
	pid3 := heap.NewHeapPageID(1, 3)

	// Add three pages
	cache.Put(pid1, newMockPage(pid1))
	cache.Put(pid2, newMockPage(pid2))
	cache.Put(pid3, newMockPage(pid3))

	// Access pid1 to make it recently used
	cache.Get(pid1)

	// Update pid2 to make it recently used
	cache.Put(pid2, newMockPage(pid2))

	// GetAll should show pid3 as LRU, then pid1, then pid2 as MRU
	pids := cache.GetAll()

	if !pids[0].Equals(pid3) {
		t.Errorf("Expected pid3 to be LRU, got %v", pids[0])
	}

	if !pids[2].Equals(pid2) {
		t.Errorf("Expected pid2 to be MRU, got %v", pids[2])
	}
}

func TestLRUPageCache_ConcurrentOperations(t *testing.T) {
	cache := NewLRUPageCache(100)
	numGoroutines := 10
	opsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent Puts
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				pid := heap.NewHeapPageID(id, j)
				p := newMockPage(pid)
				cache.Put(pid, p)
			}
		}(i)
	}

	wg.Wait()

	// Verify cache integrity
	if cache.Size() > 100 {
		t.Errorf("Cache exceeded max size: %d", cache.Size())
	}
}

func TestLRUPageCache_ConcurrentGetPut(t *testing.T) {
	cache := NewLRUPageCache(50)
	numGoroutines := 5

	// Pre-populate cache
	for i := 0; i < 20; i++ {
		pid := heap.NewHeapPageID(1, i)
		p := newMockPage(pid)
		cache.Put(pid, p)
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Concurrent Gets
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				pid := heap.NewHeapPageID(1, j%20)
				cache.Get(pid)
			}
		}()
	}

	// Concurrent Puts
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				pid := heap.NewHeapPageID(2, id*30+j)
				p := newMockPage(pid)
				cache.Put(pid, p)
			}
		}(i)
	}

	wg.Wait()

	// Verify cache integrity
	if cache.Size() > 50 {
		t.Errorf("Cache exceeded max size: %d", cache.Size())
	}
}

func TestLRUPageCache_ConcurrentRemove(t *testing.T) {
	cache := NewLRUPageCache(100)

	// Store page IDs so we can reuse them
	pids := make([]primitives.PageID, 50)

	// Pre-populate cache
	for i := 0; i < 50; i++ {
		pid := heap.NewHeapPageID(1, i)
		pids[i] = pid
		p := newMockPage(pid)
		cache.Put(pid, p)
	}

	var wg sync.WaitGroup
	wg.Add(10)

	// Concurrent Removes
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				cache.Remove(pids[id*5+j])
			}
		}(i)
	}

	wg.Wait()

	// All 50 pages should be removed
	if cache.Size() != 0 {
		t.Errorf("Expected size=0 after removes, got %d", cache.Size())
	}
}

func TestLRUPageCache_ConcurrentClear(t *testing.T) {
	cache := NewLRUPageCache(100)

	var wg sync.WaitGroup
	wg.Add(11)

	// Concurrent Puts
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				pid := heap.NewHeapPageID(id, j)
				p := newMockPage(pid)
				cache.Put(pid, p)
			}
		}(i)
	}

	// Concurrent Clear
	go func() {
		defer wg.Done()
		cache.Clear()
	}()

	wg.Wait()

	// After clear, size should be deterministic (either 0 or some pages added after clear)
	finalSize := cache.Size()
	if finalSize < 0 || finalSize > 100 {
		t.Errorf("Invalid cache size after concurrent clear: %d", finalSize)
	}
}

func TestLRUPageCache_MultipleTablePages(t *testing.T) {
	cache := NewLRUPageCache(10)

	// Store page IDs so we can reuse them
	pids := make(map[string]primitives.PageID)

	// Add pages from different tables
	for table := 1; table <= 3; table++ {
		for pageNo := 1; pageNo <= 2; pageNo++ {
			pid := heap.NewHeapPageID(table, pageNo)
			key := fmt.Sprintf("%d-%d", table, pageNo)
			pids[key] = pid
			p := newMockPage(pid)
			err := cache.Put(pid, p)
			if err != nil {
				t.Errorf("Put failed for table %d page %d: %v", table, pageNo, err)
			}
		}
	}

	if cache.Size() != 6 {
		t.Errorf("Expected size=6, got %d", cache.Size())
	}

	// Verify all pages can be retrieved
	for table := 1; table <= 3; table++ {
		for pageNo := 1; pageNo <= 2; pageNo++ {
			key := fmt.Sprintf("%d-%d", table, pageNo)
			pid := pids[key]
			_, found := cache.Get(pid)
			if !found {
				t.Errorf("Page not found for table %d page %d", table, pageNo)
			}
		}
	}
}
