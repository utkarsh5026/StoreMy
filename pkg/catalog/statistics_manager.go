package catalog

import (
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/concurrency/transaction"
	"sync"
	"time"
)

// StatisticsManager handles automatic statistics updates with intelligent caching
type StatisticsManager struct {
	catalog           *catalogmanager.CatalogManager
	lastUpdate        map[int]time.Time // tableID -> last update time
	modificationCount map[int]int       // tableID -> number of modifications since last stats update
	mu                sync.RWMutex
	updateThreshold   int            // Number of modifications before forcing stats update
	updateInterval    time.Duration  // Minimum time between stats updates
	stopChan          chan struct{}  // Channel to signal background worker to stop
	wg                sync.WaitGroup // WaitGroup to track background worker
	db                interface {
		BeginTransaction() (*transaction.TransactionContext, error)
		CommitTransaction(tx *transaction.TransactionContext) error
	} // Database interface for creating and committing transactions
}

// NewStatisticsManager creates a new statistics manager
func NewStatisticsManager(catalog *catalogmanager.CatalogManager, db interface {
	BeginTransaction() (*transaction.TransactionContext, error)
	CommitTransaction(tx *transaction.TransactionContext) error
}) *StatisticsManager {
	return &StatisticsManager{
		catalog:           catalog,
		db:                db,
		lastUpdate:        make(map[int]time.Time),
		modificationCount: make(map[int]int),
		updateThreshold:   1000, // Update stats after 1000 modifications
		updateInterval:    5 * time.Minute,
		stopChan:          make(chan struct{}),
	}
}

// RecordModification records a modification to a table (insert/delete/update)
// This is called by the PageStore after successful modifications
func (sm *StatisticsManager) RecordModification(tableID int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.modificationCount[tableID]++
}

// ShouldUpdateStatistics determines if statistics should be updated based on
// modification count and time since last update
func (sm *StatisticsManager) ShouldUpdateStatistics(tableID int) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	modCount := sm.modificationCount[tableID]
	lastUpdate, exists := sm.lastUpdate[tableID]

	if modCount >= sm.updateThreshold {
		return true
	}

	if exists && time.Since(lastUpdate) >= sm.updateInterval {
		return true
	}

	return !exists
}

// UpdateStatisticsIfNeeded updates statistics only if needed based on heuristics
func (sm *StatisticsManager) UpdateStatisticsIfNeeded(tx *transaction.TransactionContext, tableID int) error {
	if !sm.ShouldUpdateStatistics(tableID) {
		return nil
	}

	// Update table-level statistics
	if err := sm.catalog.UpdateTableStatistics(tx, tableID); err != nil {
		return err
	}

	// Update column-level statistics
	if err := sm.catalog.UpdateColumnStatistics(tx, tableID); err != nil {
		// Non-fatal: log but continue
		// In production, you'd use proper logging
	}

	// Update index statistics
	if err := sm.catalog.UpdateIndexStatistics(tx, tableID); err != nil {
		// Non-fatal: log but continue
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.lastUpdate[tableID] = time.Now()
	sm.modificationCount[tableID] = 0

	return nil
}

// ForceUpdate forces an immediate statistics update regardless of heuristics
func (sm *StatisticsManager) ForceUpdate(tx *transaction.TransactionContext, tableID int) error {
	if err := sm.catalog.UpdateTableStatistics(tx, tableID); err != nil {
		return err
	}

	// Update column-level statistics
	if err := sm.catalog.UpdateColumnStatistics(tx, tableID); err != nil {
		// Non-fatal: log but continue
	}

	// Update index statistics
	if err := sm.catalog.UpdateIndexStatistics(tx, tableID); err != nil {
		// Non-fatal: log but continue
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.lastUpdate[tableID] = time.Now()
	sm.modificationCount[tableID] = 0

	return nil
}

// SetUpdateThreshold sets the modification threshold for automatic updates
func (sm *StatisticsManager) SetUpdateThreshold(threshold int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.updateThreshold = threshold
}

// SetUpdateInterval sets the time interval for automatic updates
func (sm *StatisticsManager) SetUpdateInterval(interval time.Duration) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.updateInterval = interval
}

// GetModificationCount returns the number of modifications for a table (for testing)
func (sm *StatisticsManager) GetModificationCount(tableID int) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.modificationCount[tableID]
}

// StartBackgroundUpdater starts a background goroutine that periodically updates statistics
// for tables that have exceeded their update threshold or time interval
func (sm *StatisticsManager) StartBackgroundUpdater(checkInterval time.Duration) {
	sm.wg.Add(1)
	go sm.backgroundUpdateLoop(checkInterval)
}

// backgroundUpdateLoop is the main loop for the background statistics updater
func (sm *StatisticsManager) backgroundUpdateLoop(checkInterval time.Duration) {
	defer sm.wg.Done()
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sm.processTableUpdates()
		case <-sm.stopChan:
			return
		}
	}
}

// processTableUpdates checks all tables with modifications and updates stats if needed
func (sm *StatisticsManager) processTableUpdates() {
	tablesToUpdate := sm.getTablesNeedingUpdate()

	for _, tableID := range tablesToUpdate {
		if err := sm.updateTableInSeparateTransaction(tableID); err != nil {
			continue
		}
	}
}

// getTablesNeedingUpdate returns a list of table IDs that need statistics updates
func (sm *StatisticsManager) getTablesNeedingUpdate() []int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var tables []int
	for tableID := range sm.modificationCount {
		if sm.shouldUpdateStatisticsLocked(tableID) {
			tables = append(tables, tableID)
		}
	}
	return tables
}

// shouldUpdateStatisticsLocked is the internal version that assumes lock is already held
func (sm *StatisticsManager) shouldUpdateStatisticsLocked(tableID int) bool {
	modCount := sm.modificationCount[tableID]
	lastUpdate, exists := sm.lastUpdate[tableID]

	if modCount >= sm.updateThreshold {
		return true
	}

	if exists && time.Since(lastUpdate) >= sm.updateInterval {
		return true
	}

	return !exists && modCount > 0
}

// updateTableInSeparateTransaction updates statistics for a table in its own transaction
func (sm *StatisticsManager) updateTableInSeparateTransaction(tableID int) error {
	tx, err := sm.db.BeginTransaction()
	if err != nil {
		return err
	}

	// Update statistics
	err = sm.catalog.UpdateTableStatistics(tx, tableID)
	if err != nil {
		return err
	}

	if err := sm.db.CommitTransaction(tx); err != nil {
		return err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.lastUpdate[tableID] = time.Now()
	sm.modificationCount[tableID] = 0

	return nil
}

// Stop gracefully stops the background updater
func (sm *StatisticsManager) Stop() {
	close(sm.stopChan)
	sm.wg.Wait()
}
