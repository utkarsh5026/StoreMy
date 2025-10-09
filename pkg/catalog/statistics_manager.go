package catalog

import (
	"storemy/pkg/concurrency/transaction"
	"sync"
	"time"
)

// StatisticsManager handles automatic statistics updates with intelligent caching
type StatisticsManager struct {
	catalog           *SystemCatalog
	lastUpdate        map[int]time.Time // tableID -> last update time
	modificationCount map[int]int       // tableID -> number of modifications since last stats update
	mu                sync.RWMutex
	updateThreshold   int           // Number of modifications before forcing stats update
	updateInterval    time.Duration // Minimum time between stats updates
}

// NewStatisticsManager creates a new statistics manager
func NewStatisticsManager(catalog *SystemCatalog) *StatisticsManager {
	return &StatisticsManager{
		catalog:           catalog,
		lastUpdate:        make(map[int]time.Time),
		modificationCount: make(map[int]int),
		updateThreshold:   1000, // Update stats after 1000 modifications
		updateInterval:    5 * time.Minute,
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

	if err := sm.catalog.UpdateTableStatistics(tx, tableID); err != nil {
		return err
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
