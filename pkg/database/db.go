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
	"sync"
)

const (
	CatalogTablesFile = "catalog_tables.dat"
)

// Database represents the main database engine that coordinates all components
type Database struct {
	tableManager *memory.TableManager
	pageStore    *memory.PageStore
	queryPlanner *planner.QueryPlanner
	catalog      *catalog.SystemCatalog

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

	tableManager := memory.NewTableManager()
	wal, err := log.NewWAL(logDir, 8192)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %v", err)
	}

	pageStore := memory.NewPageStore(tableManager, wal)
	queryPlanner := planner.NewQueryPlanner(tableManager, pageStore)

	systemCatalog := catalog.NewSystemCatalog(pageStore, tableManager)

	db := &Database{
		tableManager: tableManager,
		pageStore:    pageStore,
		queryPlanner: queryPlanner,
		catalog:      systemCatalog,
		name:         name,
		dataDir:      fullPath,
		stats:        &DatabaseStats{},
	}

	if err := db.loadExistingTables(); err != nil {
		return nil, fmt.Errorf("failed to load existing tables: %v", err)
	}
	return db, nil
}

func (db *Database) ExecuteQuery(query string) (QueryResult, error) {
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		db.recordError()
		return QueryResult{}, fmt.Errorf("parse error: %v", err)
	}

	tid := transaction.NewTransactionID()
	defer db.cleanupTransaction(tid, &err)

	plan, err := db.queryPlanner.Plan(stmt, tid)
	if err != nil {
		db.recordError()
		return QueryResult{}, fmt.Errorf("planning error: %v", err)
	}

	result, err := db.executePlan(plan, stmt)
	if err != nil {
		db.recordError()
		return QueryResult{}, fmt.Errorf("execution error: %v", err)
	}

	if err := db.pageStore.CommitTransaction(tid); err != nil {
		db.recordError()
		return QueryResult{}, fmt.Errorf("commit error: %v", err)
	}

	db.recordSuccess()
	return result, nil
}

// loadExistingTables loads table metadata from disk
func (db *Database) loadExistingTables() error {
	catalogTablesPath := filepath.Join(db.dataDir, CatalogTablesFile)

	if _, err := os.Stat(catalogTablesPath); os.IsNotExist(err) {
		if err := db.catalog.Initialize(db.dataDir); err != nil {
			return fmt.Errorf("failed to initialize catalog: %v", err)
		}
		return nil
	}

	if err := db.catalog.Initialize(db.dataDir); err != nil {
		return fmt.Errorf("failed to initialize catalog: %v", err)
	}

	if err := db.catalog.LoadTables(db.dataDir); err != nil {
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

	return db.tableManager.GetAllTableNames()
}

// GetCatalog returns the system catalog instance
func (db *Database) GetCatalog() *catalog.SystemCatalog {
	return db.catalog
}

// GetStatistics returns current database statistics
func (db *Database) GetStatistics() DatabaseInfo {
	db.stats.mutex.RLock()
	defer db.stats.mutex.RUnlock()

	tables := db.GetTables()

	return DatabaseInfo{
		Name:              db.name,
		Tables:            tables,
		TableCount:        len(tables),
		QueriesExecuted:   db.stats.QueriesExecuted,
		TransactionsCount: db.stats.TransactionsCount,
		ErrorCount:        db.stats.ErrorCount,
	}
}

func (db *Database) cleanupTransaction(tid *transaction.TransactionID, err *error) {
	if *err != nil {
		db.pageStore.AbortTransaction(tid)
	}
	db.stats.mutex.Lock()
	db.stats.TransactionsCount++
	db.stats.mutex.Unlock()
}

func (db *Database) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if err := db.pageStore.FlushAllPages(); err != nil {
		return fmt.Errorf("failed to flush pages: %v", err)
	}

	db.tableManager.Clear()
	return nil
}
