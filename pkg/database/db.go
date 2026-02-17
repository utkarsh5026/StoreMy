package database

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/catalog"
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	dberror "storemy/pkg/error"
	"storemy/pkg/log/wal"
	"storemy/pkg/logging"
	"storemy/pkg/memory"
	"storemy/pkg/parser/parser"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner"
	"storemy/pkg/registry"
	"sync"
	"time"
)

const (
	CatalogTablesFile = "catalog_tables.dat"
)

// Database represents the main database engine that coordinates all components
type Database struct {
	catalogMgr   *catalogmanager.CatalogManager
	pageStore    *memory.PageStore
	queryPlanner *planner.QueryPlanner
	walInstance  *wal.WAL
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
	log := logging.WithComponent("database").With("database", name)
	log.Info("initializing database", "data_dir", dataDir, "log_dir", logDir)

	fullPath := filepath.Join(dataDir, name)
	if err := os.MkdirAll(fullPath, 0o750); err != nil {
		dbErr := dberror.Wrap(err, "DIR_CREATE_FAILED", "NewDatabase", "Database")
		dbErr.Detail = fmt.Sprintf("Failed to create directory: %s", fullPath)
		dbErr.Hint = "Check that the parent directory exists and you have write permissions"
		log.Error("failed to create database directory", "error", err, "path", fullPath)
		return nil, dbErr
	}

	walInstance, err := wal.NewWAL(logDir, 8192)
	if err != nil {
		dbErr := dberror.Wrap(err, "WAL_INIT_FAILED", "NewDatabase", "WAL")
		dbErr.Detail = fmt.Sprintf("Failed to initialize Write-Ahead Log at: %s", logDir)
		dbErr.Hint = "Ensure the log directory exists and has sufficient disk space"
		log.Error("WAL initialization failed", "error", err, "log_dir", logDir)
		return nil, dbErr
	}
	log.Debug("WAL initialized", "log_dir", logDir)

	pageStore := memory.NewPageStore(walInstance)
	catalogMgr := catalogmanager.NewCatalogManager(pageStore, fullPath)

	ctx := registry.NewDatabaseContext(pageStore, catalogMgr, walInstance, fullPath)

	db := &Database{
		catalogMgr:  catalogMgr,
		pageStore:   pageStore,
		txRegistry:  ctx.TransactionRegistry(),
		walInstance: walInstance,
		name:        name,
		dataDir:     fullPath,
		stats:       &DatabaseStats{},
	}

	statsManager := catalog.NewStatisticsManager(catalogMgr, db)
	db.statsManager = statsManager

	queryPlanner := planner.NewQueryPlanner(ctx)
	db.queryPlanner = queryPlanner

	statsManager.StartBackgroundUpdater(30 * time.Second)
	log.Info("statistics background updater started", "interval_seconds", 30)

	if err := db.loadExistingTables(); err != nil {
		dbErr := dberror.Wrap(err, "TABLE_LOAD_FAILED", "NewDatabase", "CatalogManager")
		dbErr.Detail = "Failed to load existing tables from disk"
		dbErr.Hint = "Database catalog files may be corrupted. Consider restoring from backup"
		log.Error("failed to load existing tables", "error", err)
		return nil, dbErr
	}

	log.Info("database initialized successfully")
	return db, nil
}

func (db *Database) ExecuteQuery(query string) (QueryResult, error) {
	var err error
	var res QueryResult

	log := logging.WithComponent("database").With("database", db.name)
	log.Info("executing query", "query_length", len(query))
	startTime := time.Now()

	tx, err := db.txRegistry.Begin()
	if err != nil {
		dbErr := dberror.Wrap(err, "TX_BEGIN_FAILED", "ExecuteQuery", "TransactionRegistry")
		dbErr.Category = dberror.ErrCategoryTransient
		dbErr.Detail = "Failed to start a new transaction"
		dbErr.Hint = "Try again. If the problem persists, check system resources"
		log.Error("transaction begin failed", "error", err)
		return res, dbErr
	}
	defer db.cleanupTransaction(tx, &err)

	txLog := logging.WithTx(int(tx.ID.ID())).With("component", "database")

	stmt, err := parser.ParseStatement(query)
	if err != nil {
		db.recordError()
		dbErr := dberror.Wrap(err, "PARSE_ERROR", "ExecuteQuery", "Parser")
		dbErr.Category = dberror.ErrCategoryUser
		dbErr.Detail = fmt.Sprintf("Invalid SQL syntax in query: %s", query)
		dbErr.Hint = "Check your SQL syntax. Common issues: missing semicolon, typos in keywords, unmatched quotes"
		txLog.Error("parse error", "error", err)
		return QueryResult{}, dbErr
	}

	var plan planner.Plan
	plan, err = db.queryPlanner.Plan(stmt, tx)
	if err != nil {
		db.recordError()
		dbErr := dberror.Wrap(err, "PLAN_ERROR", "ExecuteQuery", "QueryPlanner")
		dbErr.Category = dberror.ErrCategoryUser
		dbErr.Detail = "Failed to create query execution plan"
		dbErr.Hint = "Verify that all referenced tables and columns exist"
		txLog.Error("planning failed", "error", err)
		return QueryResult{}, dbErr
	}

	var result QueryResult
	result, err = db.executePlan(plan, stmt)
	if err != nil {
		db.recordError()
		dbErr := dberror.Wrap(err, "EXEC_ERROR", "ExecuteQuery", "Executor")
		dbErr.Detail = "Failed to execute query plan"
		txLog.Error("execution failed", "error", err)
		return QueryResult{}, dbErr
	}

	err = db.pageStore.CommitTransaction(tx)
	if err != nil {
		db.recordError()
		dbErr := dberror.Wrap(err, "COMMIT_FAILED", "ExecuteQuery", "PageStore")
		dbErr.Category = dberror.ErrCategoryTransient
		dbErr.Detail = "Failed to commit transaction changes to disk"
		dbErr.Hint = "This may be a temporary issue. Retry the operation"
		txLog.Error("commit failed", "error", err)
		return QueryResult{}, dbErr
	}

	elapsed := time.Since(startTime).Milliseconds()
	db.recordSuccess()
	txLog.Info("query completed successfully", "duration_ms", elapsed, "rows_affected", result.RowsAffected)
	return result, nil
}

// loadExistingTables loads table metadata from disk
func (db *Database) loadExistingTables() error {
	log := logging.WithComponent("database").With("database", db.name)
	log.Info("loading existing tables", "data_dir", db.dataDir)

	tx, err := db.txRegistry.Begin()
	if err != nil {
		dbErr := dberror.Wrap(err, "TX_BEGIN_FAILED", "loadExistingTables", "TransactionRegistry")
		dbErr.Category = dberror.ErrCategorySystem
		dbErr.Detail = "Failed to begin transaction for catalog initialization"
		log.Error("transaction begin failed for catalog initialization", "error", err)
		return dbErr
	}

	if err := db.catalogMgr.Initialize(tx); err != nil {
		dbErr := dberror.Wrap(err, "CATALOG_INIT_FAILED", "loadExistingTables", "CatalogManager")
		dbErr.Category = dberror.ErrCategorySystem
		dbErr.Detail = "Failed to initialize system catalog tables"
		dbErr.Hint = "The database may need to be recreated if catalog tables are corrupted"
		log.Error("catalog initialization failed", "error", err)
		return dbErr
	}
	log.Info("catalog initialized successfully")

	catalogTablesPath := filepath.Join(db.dataDir, CatalogTablesFile)
	log.Debug("checking for catalog file", "path", catalogTablesPath)
	if _, err := os.Stat(catalogTablesPath); os.IsNotExist(err) {
		log.Info("catalog file does not exist, skipping table loading")
		return nil
	}
	log.Info("catalog file exists, loading tables")

	tx2, err := db.txRegistry.Begin()
	if err != nil {
		dbErr := dberror.Wrap(err, "TX_BEGIN_FAILED", "loadExistingTables", "TransactionRegistry")
		dbErr.Category = dberror.ErrCategorySystem
		dbErr.Detail = "Failed to begin transaction for loading tables"
		log.Error("transaction begin failed for loading tables", "error", err)
		return dbErr
	}

	if err := db.catalogMgr.LoadAllTables(tx2); err != nil {
		dbErr := dberror.Wrap(err, "TABLE_LOAD_FAILED", "loadExistingTables", "CatalogManager")
		dbErr.Category = dberror.ErrCategoryData
		dbErr.Detail = fmt.Sprintf("Failed to load tables from catalog file: %s", catalogTablesPath)
		dbErr.Hint = "Catalog file may be corrupted. Check file permissions and integrity"
		log.Error("failed to load tables from catalog", "error", err, "path", catalogTablesPath)
		return dbErr
	}
	log.Info("tables loaded successfully")

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

	tx, _ := db.txRegistry.Begin()
	defer func() { _ = db.pageStore.CommitTransaction(tx) }()
	names, _ := db.catalogMgr.ListAllTables(tx, true)
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
	log := logging.WithTx(int(tx.ID.ID())).With("component", "database")

	if *err != nil {
		log.Debug("aborting transaction due to error")
		if abortErr := db.pageStore.AbortTransaction(tx); abortErr != nil {
			log.Error("failed to abort transaction", "error", abortErr)
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
		dbErr := dberror.Wrap(err, "TX_BEGIN_FAILED", "UpdateTableStatistics", "TransactionRegistry")
		dbErr.Category = dberror.ErrCategoryTransient
		dbErr.Detail = "Failed to begin transaction for statistics update"
		return dbErr
	}
	defer db.cleanupTransaction(tx, &err)

	tableID, err := db.catalogMgr.GetTableID(tx, tableName)
	if err != nil {
		dbErr := dberror.Wrap(err, "TABLE_NOT_FOUND", "UpdateTableStatistics", "CatalogManager")
		dbErr.Category = dberror.ErrCategoryUser
		dbErr.Detail = fmt.Sprintf("Table '%s' does not exist", tableName)
		dbErr.Hint = "Use SHOW TABLES to list available tables"
		return dbErr
	}

	if err := db.catalogMgr.UpdateTableStatistics(tx, tableID); err != nil {
		dbErr := dberror.Wrap(err, "STATS_UPDATE_FAILED", "UpdateTableStatistics", "CatalogManager")
		dbErr.Category = dberror.ErrCategorySystem
		dbErr.Detail = fmt.Sprintf("Failed to update statistics for table '%s'", tableName)
		return dbErr
	}

	return db.pageStore.CommitTransaction(tx)
}

// GetTableStatistics returns statistics for a specific table
func (db *Database) GetTableStatistics(tableName string) (*systemtable.TableStatistics, error) {
	tx, err := db.txRegistry.Begin()
	if err != nil {
		dbErr := dberror.Wrap(err, "TX_BEGIN_FAILED", "GetTableStatistics", "TransactionRegistry")
		dbErr.Category = dberror.ErrCategoryTransient
		dbErr.Detail = "Failed to begin transaction for reading statistics"
		return nil, dbErr
	}
	defer func() {
		if err != nil {
			_ = db.pageStore.AbortTransaction(tx)
		}
	}()

	tableID, err := db.catalogMgr.GetTableID(tx, tableName)
	if err != nil {
		dbErr := dberror.Wrap(err, "TABLE_NOT_FOUND", "GetTableStatistics", "CatalogManager")
		dbErr.Category = dberror.ErrCategoryUser
		dbErr.Detail = fmt.Sprintf("Table '%s' does not exist", tableName)
		dbErr.Hint = "Use SHOW TABLES to list available tables"
		return nil, dbErr
	}

	stats, err := db.catalogMgr.GetTableStatistics(tx, tableID)
	if err != nil {
		dbErr := dberror.Wrap(err, "STATS_READ_FAILED", "GetTableStatistics", "CatalogManager")
		dbErr.Category = dberror.ErrCategorySystem
		dbErr.Detail = fmt.Sprintf("Failed to read statistics for table '%s'", tableName)
		return nil, dbErr
	}

	return stats, nil
}

// BeginTransaction starts a new transaction
func (db *Database) BeginTransaction() (*transaction.TransactionContext, error) {
	log := logging.WithComponent("database").With("database", db.name)
	tx, err := db.txRegistry.Begin()
	if err != nil {
		log.Error("failed to begin transaction", "error", err)
		return nil, err
	}
	log.Info("transaction started", "tx_id", tx.ID.ID())
	return tx, nil
}

// CommitTransaction commits a transaction
func (db *Database) CommitTransaction(tx *transaction.TransactionContext) error {
	log := logging.WithTx(int(tx.ID.ID())).With("component", "database")
	log.Info("committing transaction")

	err := db.pageStore.CommitTransaction(tx)
	if err != nil {
		log.Error("commit failed", "error", err)
		return err
	}

	log.Info("transaction committed successfully")
	return nil
}

func (db *Database) Close() error {
	log := logging.WithComponent("database").With("database", db.name)
	log.Info("closing database")

	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.statsManager != nil {
		log.Debug("stopping statistics manager")
		db.statsManager.Stop()
	}

	log.Debug("flushing all pages")
	if err := db.pageStore.FlushAllPages(); err != nil {
		dbErr := dberror.Wrap(err, "PAGE_FLUSH_FAILED", "Close", "PageStore")
		dbErr.Category = dberror.ErrCategorySystem
		dbErr.Detail = "Failed to flush dirty pages to disk during shutdown"
		dbErr.Hint = "Check disk space and file system health. Some data may not be persisted"
		log.Error("failed to flush pages", "error", err)
		return dbErr
	}

	log.Debug("closing WAL")
	if err := db.walInstance.Close(); err != nil {
		dbErr := dberror.Wrap(err, "WAL_CLOSE_FAILED", "Close", "WAL")
		dbErr.Category = dberror.ErrCategorySystem
		dbErr.Detail = "Failed to close Write-Ahead Log during shutdown"
		log.Error("failed to close WAL", "error", err)
		return dbErr
	}

	log.Debug("clearing catalog cache")
	db.catalogMgr.ClearCache()

	log.Info("database closed successfully")
	return nil
}
