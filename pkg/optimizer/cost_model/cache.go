package costmodel

import (
	"sync"
	"time"
)

// BufferPoolCache models the effect of the buffer pool on repeated scans.
// When the same table is scanned multiple times within a query or transaction,
// pages may already be in the buffer pool, reducing I/O cost.
//
// This is a simplified model that tracks recent table accesses and applies
// a discount factor to I/O costs for recently accessed tables.
type BufferPoolCache struct {
	mu            sync.RWMutex
	recentAccess  map[int]time.Time // tableID -> last access time
	cacheWindow   time.Duration     // How long to consider data "cached"
	cacheDiscount float64           // Cost multiplier for cached data (0.0-1.0)
}

// NewBufferPoolCache creates a new buffer pool cache model.
// - cacheWindow: Duration to consider pages cached (e.g., 5 seconds)
// - cacheDiscount: I/O cost multiplier for cached pages (0.1 = 90% discount)
func NewBufferPoolCache(cacheWindow time.Duration, cacheDiscount float64) *BufferPoolCache {
	return &BufferPoolCache{
		recentAccess:  make(map[int]time.Time),
		cacheWindow:   cacheWindow,
		cacheDiscount: cacheDiscount,
	}
}

// RecordAccess records that a table was accessed at the current time.
func (bpc *BufferPoolCache) RecordAccess(tableID int) {
	bpc.mu.Lock()
	defer bpc.mu.Unlock()
	bpc.recentAccess[tableID] = time.Now()
}

// GetCacheFactor returns the I/O cost multiplier for a table.
// Returns a value between cacheDiscount and 1.0:
// - cacheDiscount: table was accessed very recently (likely in cache)
// - 1.0: table hasn't been accessed recently (cold read from disk)
func (bpc *BufferPoolCache) GetCacheFactor(tableID int) float64 {
	bpc.mu.RLock()
	defer bpc.mu.RUnlock()

	lastAccess, exists := bpc.recentAccess[tableID]
	if !exists {
		// Never accessed: cold read
		return 1.0
	}

	elapsed := time.Since(lastAccess)
	if elapsed > bpc.cacheWindow {
		// Outside cache window: assume evicted
		return 1.0
	}

	// Linear interpolation between cacheDiscount and 1.0
	// based on time since last access
	ratio := float64(elapsed) / float64(bpc.cacheWindow)
	return bpc.cacheDiscount + (1.0-bpc.cacheDiscount)*ratio
}

// Clear removes all cache entries (useful for transaction boundaries).
func (bpc *BufferPoolCache) Clear() {
	bpc.mu.Lock()
	defer bpc.mu.Unlock()
	bpc.recentAccess = make(map[int]time.Time)
}

// CleanExpired removes entries older than the cache window.
// This should be called periodically to prevent unbounded memory growth.
func (bpc *BufferPoolCache) CleanExpired() {
	bpc.mu.Lock()
	defer bpc.mu.Unlock()

	now := time.Now()
	for tableID, lastAccess := range bpc.recentAccess {
		if now.Sub(lastAccess) > bpc.cacheWindow {
			delete(bpc.recentAccess, tableID)
		}
	}
}

// ApplyCacheFactor applies the cache discount to an I/O cost.
// This should be called when estimating costs for table scans.
func (cm *CostModel) applyCacheFactor(tableID int, ioCost float64) float64 {
	if cm.bufferCache == nil {
		// No cache modeling enabled
		return ioCost
	}

	cacheFactor := cm.bufferCache.GetCacheFactor(tableID)
	return ioCost * cacheFactor
}

// recordTableAccess records that a table was accessed during costing.
// This updates the buffer pool cache model.
func (cm *CostModel) recordTableAccess(tableID int) {
	if cm.bufferCache != nil {
		cm.bufferCache.RecordAccess(tableID)
	}
}

// EnableBufferPoolCaching enables buffer pool cache modeling.
// This should be called once when setting up the cost model.
// - cacheWindow: How long to consider data cached (e.g., 5*time.Second)
// - cacheDiscount: I/O cost multiplier for cached data (e.g., 0.1 for 90% discount)
func (cm *CostModel) EnableBufferPoolCaching(cacheWindow time.Duration, cacheDiscount float64) {
	cm.bufferCache = NewBufferPoolCache(cacheWindow, cacheDiscount)
}

// ClearBufferCache clears the buffer cache model.
// Useful at transaction boundaries.
func (cm *CostModel) ClearBufferCache() {
	if cm.bufferCache != nil {
		cm.bufferCache.Clear()
	}
}
