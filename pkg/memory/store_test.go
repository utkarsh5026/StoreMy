package memory

import (
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/types"
	"sync"
	"testing"
)

// TestNewPageStore tests PageStore initialization
func TestNewPageStore(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)

	if ps == nil {
		t.Fatal("NewPageStore returned nil")
	}

	if ps.cache == nil {
		t.Error("Cache not initialized")
	}

	if ps.lockManager == nil {
		t.Error("LockManager not initialized")
	}

	if ps.wal != wal {
		t.Error("WAL not set correctly")
	}

	if ps.dbFiles == nil {
		t.Error("dbFiles map not initialized")
	}
}

// TestRegisterDbFile tests DbFile registration
func TestRegisterDbFile(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})

	ps.RegisterDbFile(1, dbFile)

	ps.mutex.RLock()
	registeredFile, exists := ps.dbFiles[1]
	ps.mutex.RUnlock()

	if !exists {
		t.Error("DbFile not registered")
	}

	if registeredFile != dbFile {
		t.Error("Registered DbFile doesn't match")
	}
}

// TestUnregisterDbFile tests DbFile removal
func TestUnregisterDbFile(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})

	ps.RegisterDbFile(1, dbFile)
	ps.UnregisterDbFile(1)

	ps.mutex.RLock()
	_, exists := ps.dbFiles[1]
	ps.mutex.RUnlock()

	if exists {
		t.Error("DbFile still registered after unregister")
	}
}

// TestGetPage_NilContext tests GetPage with nil transaction context
func TestGetPage_NilContext(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	pid := page.NewPageDescriptor(1, 0)

	_, err = ps.GetPage(nil, dbFile, pid, transaction.ReadOnly)

	if err == nil {
		t.Error("Expected error for nil context")
	}
}

// TestGetPage_BasicRead tests basic page reading
func TestGetPage_BasicRead(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ctx := createTransactionContext(t, wal)
	pid := page.NewPageDescriptor(1, 0)

	page, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadOnly)

	if err != nil {
		t.Errorf("GetPage failed: %v", err)
	}

	if page == nil {
		t.Fatal("GetPage returned nil page")
	}

	if !page.GetID().Equals(pid) {
		t.Error("Page ID doesn't match")
	}

	// Verify page is in cache
	ps.mutex.RLock()
	cachedPage, exists := ps.cache.Get(pid)
	ps.mutex.RUnlock()

	if !exists {
		t.Error("Page not added to cache")
	}

	if cachedPage != page {
		t.Error("Cached page doesn't match returned page")
	}
}

// TestGetPage_CachedPage tests retrieving a page from cache
func TestGetPage_CachedPage(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ctx := createTransactionContext(t, wal)
	pid := page.NewPageDescriptor(1, 0)

	// First read - loads from disk
	page1, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadOnly)
	if err != nil {
		t.Fatalf("First GetPage failed: %v", err)
	}

	// Second read - should come from cache
	page2, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadOnly)
	if err != nil {
		t.Errorf("Second GetPage failed: %v", err)
	}

	if page1 != page2 {
		t.Error("Cache not returning same page instance")
	}
}

// TestGetPage_ReadWritePermissions tests page access with write permissions
func TestGetPage_ReadWritePermissions(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ctx := createTransactionContext(t, wal)
	pid := page.NewPageDescriptor(1, 0)

	page, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)

	if err != nil {
		t.Errorf("GetPage with ReadWrite failed: %v", err)
	}

	if page == nil {
		t.Fatal("GetPage returned nil page")
	}

	// Verify transaction recorded the access
	perm, exists := ctx.GetPagePermission(pid)
	if !exists {
		t.Error("Transaction didn't record page access")
	}

	if perm != transaction.ReadWrite {
		t.Errorf("Expected ReadWrite permission, got %v", perm)
	}
}

// TestGetPage_Eviction tests page eviction when cache is full
func TestGetPage_Eviction(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})

	// Fill the cache to MaxPageCount
	for i := 0; i < MaxPageCount; i++ {
		ctx := createTransactionContext(t, wal)
		pid := page.NewPageDescriptor(1, primitives.PageNumber(i))
		_, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadOnly)
		if err != nil {
			t.Fatalf("Failed to fill cache: %v", err)
		}
		// Release locks so pages can be evicted
		ps.lockManager.UnlockAllPages(ctx.ID)
	}

	// Try to add one more page - should trigger eviction
	ctx := createTransactionContext(t, wal)
	pid := page.NewPageDescriptor(1, MaxPageCount)
	_, err = ps.GetPage(ctx, dbFile, pid, transaction.ReadOnly)

	if err != nil {
		t.Errorf("GetPage with eviction failed: %v", err)
	}
}

// TestGetPage_EvictionFailure tests when no pages can be evicted
func TestGetPage_EvictionFailure(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ctx := createTransactionContext(t, wal)

	// Fill cache with dirty pages that can't be evicted (NO-STEAL)
	for i := 0; i < MaxPageCount; i++ {
		pid := page.NewPageDescriptor(1, primitives.PageNumber(i))
		page, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)
		if err != nil {
			t.Fatalf("Failed to fill cache: %v", err)
		}
		// Mark pages dirty so they can't be evicted
		page.MarkDirty(true, ctx.ID)
		ctx.MarkPageDirty(pid)
		ps.cache.Put(pid, page)
	}

	// Try to add another page - should fail (NO-STEAL policy)
	newCtx := createTransactionContext(t, wal)
	pid := page.NewPageDescriptor(1, MaxPageCount)
	_, err = ps.GetPage(newCtx, dbFile, pid, transaction.ReadOnly)

	if err == nil {
		t.Error("Expected error when cache is full of dirty pages")
	}
}

// TestHandlePageChange_InsertOperation tests logging insert operations
func TestHandlePageChange_InsertOperation(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	ctx := createTransactionContext(t, wal)

	// Ensure transaction is begun in WAL
	if err := ctx.EnsureBegunInWAL(wal); err != nil {
		t.Fatalf("Failed to begin transaction in WAL: %v", err)
	}

	pid := page.NewPageDescriptor(1, 0)

	getDirtyPages := func() ([]page.Page, error) {
		mockPage := newMockPage(pid)
		return []page.Page{mockPage}, nil
	}

	err = ps.HandlePageChange(ctx, InsertOperation, getDirtyPages)

	if err != nil {
		t.Errorf("HandlePageChange for INSERT failed: %v", err)
	}

	// Verify page was marked dirty in cache
	ps.mutex.RLock()
	cachedPage, exists := ps.cache.Get(pid)
	ps.mutex.RUnlock()

	if !exists {
		t.Error("Page not added to cache after insert")
	}

	if cachedPage.IsDirty() == nil {
		t.Error("Page not marked dirty after insert")
	}
}

// TestHandlePageChange_UpdateOperation tests logging update operations
func TestHandlePageChange_UpdateOperation(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	ctx := createTransactionContext(t, wal)

	// Ensure transaction is begun in WAL
	if err := ctx.EnsureBegunInWAL(wal); err != nil {
		t.Fatalf("Failed to begin transaction in WAL: %v", err)
	}

	pid := page.NewPageDescriptor(1, 0)

	// Create a page with a before image
	mockPage := newMockPage(pid)
	mockPage.SetBeforeImage()

	getDirtyPages := func() ([]page.Page, error) {
		return []page.Page{mockPage}, nil
	}

	err = ps.HandlePageChange(ctx, UpdateOperation, getDirtyPages)

	if err != nil {
		t.Errorf("HandlePageChange for UPDATE failed: %v", err)
	}
}

// TestHandlePageChange_DeleteOperation tests logging delete operations
func TestHandlePageChange_DeleteOperation(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	ctx := createTransactionContext(t, wal)

	// Ensure transaction is begun in WAL
	if err := ctx.EnsureBegunInWAL(wal); err != nil {
		t.Fatalf("Failed to begin transaction in WAL: %v", err)
	}

	pid := page.NewPageDescriptor(1, 0)

	getDirtyPages := func() ([]page.Page, error) {
		mockPage := newMockPage(pid)
		return []page.Page{mockPage}, nil
	}

	err = ps.HandlePageChange(ctx, DeleteOperation, getDirtyPages)

	if err != nil {
		t.Errorf("HandlePageChange for DELETE failed: %v", err)
	}
}

// TestHandlePageChange_NilContext tests with nil context
func TestHandlePageChange_NilContext(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)

	getDirtyPages := func() ([]page.Page, error) {
		return []page.Page{}, nil
	}

	err = ps.HandlePageChange(nil, InsertOperation, getDirtyPages)

	if err == nil {
		t.Error("Expected error with nil context")
	}
}

// TestCommitTransaction tests successful transaction commit
func TestCommitTransaction(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ps.RegisterDbFile(1, dbFile)
	ctx := createTransactionContext(t, wal)

	// Ensure transaction is begun in WAL
	if err := ctx.EnsureBegunInWAL(wal); err != nil {
		t.Fatalf("Failed to begin transaction in WAL: %v", err)
	}

	// Get a page and modify it
	pid := page.NewPageDescriptor(1, 0)
	page, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	page.MarkDirty(true, ctx.ID)
	ctx.MarkPageDirty(pid)

	// Commit the transaction
	err = ps.CommitTransaction(ctx)

	if err != nil {
		t.Errorf("CommitTransaction failed: %v", err)
	}

	// Verify page is no longer dirty
	if page.IsDirty() != nil {
		t.Error("Page still dirty after commit")
	}

	// Verify locks are released
	if ps.lockManager.IsPageLocked(pid) {
		t.Error("Page still locked after commit")
	}

	// Verify before image was set
	if page.GetBeforeImage() == nil {
		t.Error("Before image not set after commit")
	}
}

// TestCommitTransaction_NoChanges tests commit with read-only transaction
func TestCommitTransaction_NoChanges(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	ctx := createTransactionContext(t, wal)

	// Commit without any changes
	err = ps.CommitTransaction(ctx)

	if err != nil {
		t.Errorf("CommitTransaction with no changes failed: %v", err)
	}
}

// TestAbortTransaction tests transaction rollback
func TestAbortTransaction(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ps.RegisterDbFile(1, dbFile)
	ctx := createTransactionContext(t, wal)

	// Ensure transaction is begun in WAL
	if err := ctx.EnsureBegunInWAL(wal); err != nil {
		t.Fatalf("Failed to begin transaction in WAL: %v", err)
	}

	// Get a page and modify it
	pid := page.NewPageDescriptor(1, 0)
	page, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	// Set before image first
	page.SetBeforeImage()
	page.MarkDirty(true, ctx.ID)
	ctx.MarkPageDirty(pid)
	ps.cache.Put(pid, page)

	// Abort the transaction
	err = ps.AbortTransaction(ctx)

	if err != nil {
		t.Errorf("AbortTransaction failed: %v", err)
	}

	// Verify locks are released
	if ps.lockManager.IsPageLocked(pid) {
		t.Error("Page still locked after abort")
	}

	// Verify page was restored from before image
	ps.mutex.RLock()
	restoredPage, exists := ps.cache.Get(pid)
	ps.mutex.RUnlock()

	if exists && restoredPage.IsDirty() != nil {
		t.Error("Page still dirty after abort")
	}
}

// TestAbortTransaction_NoBeforeImage tests abort without before image
func TestAbortTransaction_NoBeforeImage(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	ctx := createTransactionContext(t, wal)

	// Ensure transaction is begun in WAL
	if err := ctx.EnsureBegunInWAL(wal); err != nil {
		t.Fatalf("Failed to begin transaction in WAL: %v", err)
	}

	// Add a dirty page without before image (newly allocated page)
	pid := page.NewPageDescriptor(1, 0)
	page := newMockPage(pid)
	page.MarkDirty(true, ctx.ID)
	ctx.MarkPageDirty(pid)
	ps.cache.Put(pid, page)

	// Abort should remove the page from cache
	err = ps.AbortTransaction(ctx)

	if err != nil {
		t.Errorf("AbortTransaction failed: %v", err)
	}

	// Verify page was removed from cache
	ps.mutex.RLock()
	_, exists := ps.cache.Get(pid)
	ps.mutex.RUnlock()

	if exists {
		t.Error("Page not removed from cache after abort (no before image)")
	}
}

// TestFlushAllPages tests flushing all dirty pages to disk
func TestFlushAllPages(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ps.RegisterDbFile(1, dbFile)
	ctx := createTransactionContext(t, wal)

	// Create several dirty pages
	pids := []*page.PageDescriptor{
		page.NewPageDescriptor(1, 0),
		page.NewPageDescriptor(1, 1),
		page.NewPageDescriptor(1, 2),
	}

	for _, pid := range pids {
		page, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)
		if err != nil {
			t.Fatalf("GetPage failed: %v", err)
		}
		page.MarkDirty(true, ctx.ID)
		ps.cache.Put(pid, page)
	}

	// Flush all pages
	err = ps.FlushAllPages()

	if err != nil {
		t.Errorf("FlushAllPages failed: %v", err)
	}

	// Verify all pages are clean
	for _, pid := range pids {
		ps.mutex.RLock()
		page, exists := ps.cache.Get(pid)
		ps.mutex.RUnlock()

		if !exists {
			t.Errorf("Page %v not in cache after flush", pid)
			continue
		}

		if page.IsDirty() != nil {
			t.Errorf("Page %v still dirty after flush", pid)
		}
	}
}

// TestFlushAllPages_EmptyCache tests flushing with empty cache
func TestFlushAllPages_EmptyCache(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)

	err = ps.FlushAllPages()

	if err != nil {
		t.Errorf("FlushAllPages with empty cache failed: %v", err)
	}
}

// TestClose tests proper shutdown
func TestClose(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ps.RegisterDbFile(1, dbFile)
	ctx := createTransactionContext(t, wal)

	// Add some dirty pages
	pid := page.NewPageDescriptor(1, 0)
	page, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}
	page.MarkDirty(true, ctx.ID)
	ps.cache.Put(pid, page)

	// Close should flush pages and close WAL
	err = ps.Close()

	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify page was flushed
	ps.mutex.RLock()
	cachedPage, _ := ps.cache.Get(pid)
	ps.mutex.RUnlock()

	if cachedPage != nil && cachedPage.IsDirty() != nil {
		t.Error("Dirty pages not flushed during close")
	}
}

// TestGetWal tests WAL getter
func TestGetWal(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)

	returnedWal := ps.GetWal()

	if returnedWal != wal {
		t.Error("GetWal returned incorrect WAL instance")
	}
}

// TestConcurrentGetPage tests concurrent page access
func TestConcurrentGetPage(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})

	var wg sync.WaitGroup
	numGoroutines := 10
	pagesPerGoroutine := 5

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			ctx := createTransactionContext(t, wal)

			for j := 0; j < pagesPerGoroutine; j++ {
				pid := page.NewPageDescriptor(1, primitives.PageNumber(id*pagesPerGoroutine+j))
				_, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadOnly)
				if err != nil {
					t.Errorf("Concurrent GetPage failed: %v", err)
				}
			}

			ps.lockManager.UnlockAllPages(ctx.ID)
		}(i)
	}

	wg.Wait()
}

// TestConcurrentCommitAbort tests concurrent commits and aborts
func TestConcurrentCommitAbort(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ps.RegisterDbFile(1, dbFile)

	var wg sync.WaitGroup
	numGoroutines := 20

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			ctx := createTransactionContext(t, wal)

			// Ensure transaction is begun in WAL
			if err := ctx.EnsureBegunInWAL(wal); err != nil {
				t.Errorf("Failed to begin transaction in WAL: %v", err)
				return
			}

			// Get and modify a page
			pid := page.NewPageDescriptor(1, primitives.PageNumber(id))
			page, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)
			if err != nil {
				t.Errorf("GetPage failed: %v", err)
				return
			}

			page.MarkDirty(true, ctx.ID)
			ctx.MarkPageDirty(pid)

			// Commit half, abort half
			if id%2 == 0 {
				if err := ps.CommitTransaction(ctx); err != nil {
					t.Errorf("CommitTransaction failed: %v", err)
				}
			} else {
				page.SetBeforeImage()
				ps.cache.Put(pid, page)
				if err := ps.AbortTransaction(ctx); err != nil {
					t.Errorf("AbortTransaction failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify no locks remain
	ps.mutex.RLock()
	cacheSize := ps.cache.Size()
	ps.mutex.RUnlock()

	// All committed pages should be in cache and clean
	for i := 0; i < numGoroutines; i += 2 {
		pid := page.NewPageDescriptor(1, primitives.PageNumber(i))
		if ps.lockManager.IsPageLocked(pid) {
			t.Errorf("Page %v still locked after commit", pid)
		}
	}

	if cacheSize > MaxPageCount {
		t.Errorf("Cache exceeded max size: %d", cacheSize)
	}
}

// TestLockingBehavior tests that proper locks are acquired
func TestLockingBehavior(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ctx1 := createTransactionContext(t, wal)
	ctx2 := createTransactionContext(t, wal)
	pid := page.NewPageDescriptor(1, 0)

	// Transaction 1 gets read lock
	_, err = ps.GetPage(ctx1, dbFile, pid, transaction.ReadOnly)
	if err != nil {
		t.Fatalf("First GetPage failed: %v", err)
	}

	// Transaction 2 should also be able to get read lock (shared)
	_, err = ps.GetPage(ctx2, dbFile, pid, transaction.ReadOnly)
	if err != nil {
		t.Errorf("Second GetPage with read lock failed: %v", err)
	}

	// Clean up
	ps.lockManager.UnlockAllPages(ctx1.ID)
	ps.lockManager.UnlockAllPages(ctx2.ID)
}

// TestEvictionPolicy tests NO-STEAL policy enforcement
func TestEvictionPolicy(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	ctx := createTransactionContext(t, wal)

	// Add a dirty page
	pid := page.NewPageDescriptor(1, 0)
	page := newMockPage(pid)
	page.MarkDirty(true, ctx.ID)
	ps.cache.Put(pid, page)

	// Try to evict - should fail because page is dirty
	err = ps.evictPage()

	if err == nil {
		t.Error("Expected error when trying to evict dirty page (NO-STEAL)")
	}

	// Clean the page
	page.MarkDirty(false, nil)
	ps.cache.Put(pid, page)

	// Release lock
	ps.lockManager.UnlockAllPages(ctx.ID)

	// Now eviction should succeed
	err = ps.evictPage()

	if err != nil {
		t.Errorf("Eviction of clean page failed: %v", err)
	}
}

// TestMultipleTableOperations tests operations across multiple tables
func TestMultipleTableOperations(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)

	// Register multiple tables
	dbFile1 := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	dbFile2 := newMockDbFileForPageStore(2, []types.Type{types.IntType}, []string{"value"})

	ps.RegisterDbFile(1, dbFile1)
	ps.RegisterDbFile(2, dbFile2)

	ctx := createTransactionContext(t, wal)

	// Ensure transaction is begun in WAL
	if err := ctx.EnsureBegunInWAL(wal); err != nil {
		t.Fatalf("Failed to begin transaction in WAL: %v", err)
	}

	// Access pages from both tables
	pid1 := page.NewPageDescriptor(1, 0)
	pid2 := page.NewPageDescriptor(2, 0)

	page1, err := ps.GetPage(ctx, dbFile1, pid1, transaction.ReadWrite)
	if err != nil {
		t.Errorf("GetPage for table 1 failed: %v", err)
	}

	page2, err := ps.GetPage(ctx, dbFile2, pid2, transaction.ReadWrite)
	if err != nil {
		t.Errorf("GetPage for table 2 failed: %v", err)
	}

	// Modify both pages
	page1.MarkDirty(true, ctx.ID)
	page2.MarkDirty(true, ctx.ID)
	ctx.MarkPageDirty(pid1)
	ctx.MarkPageDirty(pid2)

	// Commit should flush both
	err = ps.CommitTransaction(ctx)

	if err != nil {
		t.Errorf("Commit with multiple tables failed: %v", err)
	}
}

// TestGetDbFileForPage tests DbFile lookup
func TestGetDbFileForPage(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ps.RegisterDbFile(1, dbFile)

	pid := page.NewPageDescriptor(1, 0)

	// Should find the DbFile
	foundFile, err := ps.getDbFileForPage(pid)
	if err != nil {
		t.Errorf("getDbFileForPage failed: %v", err)
	}

	if foundFile != dbFile {
		t.Error("Wrong DbFile returned")
	}

	// Should fail for unregistered table
	pid2 := page.NewPageDescriptor(999, 0)
	_, err = ps.getDbFileForPage(pid2)

	if err == nil {
		t.Error("Expected error for unregistered table")
	}
}

// TestPageStoreStress performs stress testing
func TestPageStoreStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ps.RegisterDbFile(1, dbFile)

	var wg sync.WaitGroup
	numWorkers := 50
	operationsPerWorker := 100

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < operationsPerWorker; j++ {
				ctx := createTransactionContext(t, wal)
				pid := page.NewPageDescriptor(1, primitives.PageNumber(workerID*operationsPerWorker+j)%500)

				page, err := ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)
				if err != nil {
					continue // Cache might be full
				}

				page.MarkDirty(true, ctx.ID)
				ctx.MarkPageDirty(pid)

				// Randomly commit or abort
				if j%2 == 0 {
					ps.CommitTransaction(ctx)
				} else {
					page.SetBeforeImage()
					ps.cache.Put(pid, page)
					ps.AbortTransaction(ctx)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify system is in consistent state
	ps.mutex.RLock()
	cacheSize := ps.cache.Size()
	ps.mutex.RUnlock()

	if cacheSize > MaxPageCount {
		t.Errorf("Cache exceeded max size after stress test: %d", cacheSize)
	}
}

// TestTransactionContextIntegration tests integration with transaction context
func TestTransactionContextIntegration(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ps.RegisterDbFile(1, dbFile)
	ctx := createTransactionContext(t, wal)

	// Verify transaction tracks page access
	pid := page.NewPageDescriptor(1, 0)
	_, err = ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	// Check transaction recorded the page
	lockedPages := ctx.GetLockedPages()
	if len(lockedPages) != 1 {
		t.Errorf("Expected 1 locked page, got %d", len(lockedPages))
	}

	if !lockedPages[0].Equals(pid) {
		t.Error("Transaction didn't record correct page")
	}
}

// Benchmark tests

func BenchmarkGetPage(b *testing.B) {
	tmpDir := b.TempDir()
	walPath := filepath.Join(tmpDir, "bench.wal")
	wal, _ := wal.NewWAL(walPath, 4096)
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ctx := createTransactionContext(&testing.T{}, wal)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pid := page.NewPageDescriptor(1, primitives.PageNumber(i%100))
		ps.GetPage(ctx, dbFile, pid, transaction.ReadOnly)
	}
}

func BenchmarkCommit(b *testing.B) {
	tmpDir := b.TempDir()
	walPath := filepath.Join(tmpDir, "bench.wal")
	wal, _ := wal.NewWAL(walPath, 4096)
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ps.RegisterDbFile(1, dbFile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := createTransactionContext(&testing.T{}, wal)
		pid := page.NewPageDescriptor(1, primitives.PageNumber(i%100))
		page, _ := ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)
		page.MarkDirty(true, ctx.ID)
		ctx.MarkPageDirty(pid)
		ps.CommitTransaction(ctx)
	}
}

func BenchmarkAbort(b *testing.B) {
	tmpDir := b.TempDir()
	walPath := filepath.Join(tmpDir, "bench.wal")
	wal, _ := wal.NewWAL(walPath, 4096)
	defer wal.Close()

	ps := NewPageStore(wal)
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	ps.RegisterDbFile(1, dbFile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := createTransactionContext(&testing.T{}, wal)
		pid := page.NewPageDescriptor(1, primitives.PageNumber(i%100))
		page, _ := ps.GetPage(ctx, dbFile, pid, transaction.ReadWrite)
		page.SetBeforeImage()
		page.MarkDirty(true, ctx.ID)
		ctx.MarkPageDirty(pid)
		ps.cache.Put(pid, page)
		ps.AbortTransaction(ctx)
	}
}
