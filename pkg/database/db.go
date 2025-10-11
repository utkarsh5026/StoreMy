package database

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/catalog"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/memory"
	"storemy/pkg/parser/parser"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"sync"
	"time"
)

const (
	CatalogTablesFile = "catalog_tables.dat"
)

// Database represents the main database engine that coordinates all components
type Database struct {
	catalogMgr   *catalog.CatalogManager
	pageStore    *memory.PageStore
	queryPlanner *planner.QueryPlanner
	wal          *log.WAL
	txRegistry   *transaction.TransactionRegistry
	statsManager *catalog.StatisticsManager

	name    string
	dataDir string

	mutex sync.RWMutex
	stats *DatabaseStats
}

// DatabaseStats tracks performance metrics
type DatabaseStats struct {
	QueriesExecuted   int64
	TransactionsCount int64
	ErrorCount        int64
	mutex             sync.RWMutex
}

// QueryResult represents the result of a query execution
type QueryResult struct {
	Success      bool
	Columns      []string
	Rows         [][]string
	RowsAffected int
	Message      string
	Error        error
}

// DatabaseInfo contains database metadata
type DatabaseInfo struct {
	Name              string
	Tables            []string
	TableCount        int
	QueriesExecuted   int64
	TransactionsCount int64
	ErrorCount        int64
}

func NewDatabase(name, dataDir, logDir string) (*Database, error) {
	fullPath := filepath.Join(dataDir, name)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	wal, err := log.NewWAL(logDir, 8192)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %v", err)
	}

	pageStore := memory.NewPageStore(wal)
	catalogMgr := catalog.NewCatalogManager(pageStore, dataDir)

	ctx := registry.NewDatabaseContext(pageStore, catalogMgr, wal, fullPath)

	db := &Database{
		catalogMgr: catalogMgr,
		pageStore:  pageStore,
		txRegistry: ctx.TransactionRegistry(),
		wal:        wal,
		name:       name,
		dataDir:    fullPath,
		stats:      &DatabaseStats{},
	}

	statsManager := catalog.NewStatisticsManager(catalogMgr, db)
	pageStore.SetStatsManager(statsManager)
	db.statsManager = statsManager

	queryPlanner := planner.NewQueryPlanner(ctx)
	db.queryPlanner = queryPlanner

	statsManager.StartBackgroundUpdater(30 * time.Second)

	if err := db.loadExistingTables(); err != nil {
		return nil, fmt.Errorf("failed to load existing tables: %v", err)
	}
	return db, nil
}

func (db *Database) ExecuteQuery(query string) (QueryResult, error) {
	var err error
	var res QueryResult
	tx, err := db.txRegistry.Begin()
	if err != nil {
		return res, err
	}
	defer db.cleanupTransaction(tx, &err)

	stmt, err := parser.ParseStatement(query)
	if err != nil {
		db.recordError()
		return QueryResult{}, fmt.Errorf("parse error: %v", err)
	}

	var plan planner.Plan
	plan, err = db.queryPlanner.Plan(stmt, tx)
	if err != nil {
		db.recordError()
		return QueryResult{}, fmt.Errorf("planning error: %v", err)
	}

	var result QueryResult
	result, err = db.executePlan(plan, stmt)
	if err != nil {
		db.recordError()
		return QueryResult{}, fmt.Errorf("execution error: %v", err)
	}

	err = db.pageStore.CommitTransaction(tx)
	if err != nil {
		db.recordError()
		return QueryResult{}, fmt.Errorf("commit error: %v", err)
	}

	db.recordSuccess()
	return result, nil
}

// loadExistingTables loads table metadata from disk
func (db *Database) loadExistingTables() error {
	tx, err := db.txRegistry.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for catalog initialization: %v", err)
	}

	if err := db.catalogMgr.Initialize(tx); err != nil {
		return fmt.Errorf("failed to initialize catalog: %v", err)
	}

	catalogTablesPath := filepath.Join(db.dataDir, CatalogTablesFile)
	if _, err := os.Stat(catalogTablesPath); os.IsNotExist(err) {
		return nil
	}

	tx2, err := db.txRegistry.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for loading tables: %v", err)
	}

	if err := db.catalogMgr.LoadAllTables(tx2); err != nil {
		return fmt.Errorf("failed to load tables from catalog: %v", err)
	}

	return nil
}

// recordError updates error statistics
func (db *Database) recordError() {
	db.stats.mutex.Lock()
	db.stats.ErrorCount++
	db.stats.mutex.Unlock()
}

// recordSuccess updates success statistics
func (db *Database) recordSuccess() {
	db.stats.mutex.Lock()
	db.stats.QueriesExecuted++
	db.stats.mutex.Unlock()
}

func (db *Database) executePlan(plan planner.Plan, stmt statements.Statement) (QueryResult, error) {
	rawResult, err := plan.Execute()
	if err != nil {
		return QueryResult{}, err
	}

	return formatResult(rawResult, stmt)
}

// GetTables returns a list of all tables in the database
func (db *Database) GetTables() []string {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	names, _ := db.catalogMgr.ListAllTables(primitives.NewTransactionID(), true)
	return names
}

// GetStatistics returns current database statistics (counts only user tables)
func (db *Database) GetStatistics() DatabaseInfo {
	db.stats.mutex.RLock()
	defer db.stats.mutex.RUnlock()

	allTables := db.GetTables()
	userTables := make([]string, 0, len(allTables))

	for _, table := range allTables {
		if len(table) >= 8 && table[:8] == "CATALOG_" {
			continue
		}
		userTables = append(userTables, table)
	}

	return DatabaseInfo{
		Name:              db.name,
		Tables:            userTables,
		TableCount:        len(userTables),
		QueriesExecuted:   db.stats.QueriesExecuted,
		TransactionsCount: db.stats.TransactionsCount,
		ErrorCount:        db.stats.ErrorCount,
	}
}

func (db *Database) cleanupTransaction(tx *transaction.TransactionContext, err *error) {
	if *err != nil {
		if abortErr := db.pageStore.AbortTransaction(tx); abortErr != nil {
			// Log abort error but don't override the original error
			fmt.Printf("failed to abort transaction: %v\n", abortErr)
		}
	}
	db.stats.mutex.Lock()
	db.stats.TransactionsCount++
	db.stats.mutex.Unlock()
}

// UpdateTableStatistics manually triggers a statistics update for a table
// This is useful for forcing an update after bulk operations
func (db *Database) UpdateTableStatistics(tableName string) error {
	tx, err := db.txRegistry.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer db.cleanupTransaction(tx, &err)

	tableID, err := db.catalogMgr.GetTableID(tx.ID, tableName)
	if err != nil {
		return fmt.Errorf("table not found: %v", err)
	}

	if err := db.catalogMgr.UpdateTableStatistics(tx, tableID); err != nil {
		return fmt.Errorf("failed to update statistics: %v", err)
	}

	return db.pageStore.CommitTransaction(tx)
}

// GetTableStatistics returns statistics for a specific table
func (db *Database) GetTableStatistics(tableName string) (*catalog.TableStatistics, error) {
	tx, err := db.txRegistry.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err != nil {
			db.pageStore.AbortTransaction(tx)
		}
	}()

	tableID, err := db.catalogMgr.GetTableID(tx.ID, tableName)
	if err != nil {
		return nil, fmt.Errorf("table not found: %v", err)
	}

	stats, err := db.catalogMgr.GetTableStatistics(tx.ID, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get statistics: %v", err)
	}

	return stats, nil
}

// BeginTransaction starts a new transaction
func (db *Database) BeginTransaction() (*transaction.TransactionContext, error) {
	return db.txRegistry.Begin()
}

// CommitTransaction commits a transaction
func (db *Database) CommitTransaction(tx *transaction.TransactionContext) error {
	return db.pageStore.CommitTransaction(tx)
}

func (db *Database) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.statsManager != nil {
		db.statsManager.Stop()
	}

	if err := db.pageStore.FlushAllPages(); err != nil {
		return fmt.Errorf("failed to flush pages: %v", err)
	}

	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %v", err)
	}

	db.catalogMgr.ClearCache()
	return nil
}
