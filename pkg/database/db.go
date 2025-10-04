package database

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/parser/parser"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner"
	"sync"
)

// Database represents the main database engine that coordinates all components
type Database struct {
	tableManager *memory.TableManager
	pageStore    *memory.PageStore
	queryPlanner *planner.QueryPlanner

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

func NewDatabase(name string, dataDir string, logDir string) (*Database, error) {
	fullPath := filepath.Join(dataDir, name)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	tableManager := memory.NewTableManager()
	pageStore, err := memory.NewPageStore(tableManager, logDir, 8192)

	if err != nil {
		return nil, fmt.Errorf("failed to create page store: %v", err)
	}

	queryPlanner := planner.NewQueryPlanner(tableManager, pageStore)

	db := &Database{
		tableManager: tableManager,
		pageStore:    pageStore,
		queryPlanner: queryPlanner,
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
	// For now, we'll start with an empty database
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

	switch stmt.GetType() {
	case statements.Select:
		if queryResult, ok := rawResult.(*planner.QueryResult); ok {
			return db.formatSelectResult(queryResult), nil
		}

	case statements.Insert, statements.Update, statements.Delete:
		if dmlResult, ok := rawResult.(*planner.DMLResult); ok {
			return db.formatDMLResult(dmlResult, stmt.GetType()), nil
		}

	case statements.CreateTable, statements.DropTable:
		if ddlResult, ok := rawResult.(*planner.DDLResult); ok {
			return db.formatDDLResult(ddlResult), nil
		}
	}

	return QueryResult{
		Success: true,
		Message: "Query executed successfully",
	}, nil
}

// formatSelectResult converts query results to our standard format
func (db *Database) formatSelectResult(result *planner.QueryResult) QueryResult {
	if result == nil || result.TupleDesc == nil {
		return QueryResult{
			Success: true,
			Message: "Query returned no results",
			Rows:    [][]string{},
		}
	}

	numFields := result.TupleDesc.NumFields()
	columns := make([]string, numFields)
	for i := range numFields {
		name, _ := result.TupleDesc.GetFieldName(i)
		if name == "" {
			name = fmt.Sprintf("col_%d", i)
		}
		columns[i] = name
	}

	rows := make([][]string, 0, len(result.Tuples))
	for _, tuple := range result.Tuples {
		row := make([]string, numFields)
		for i := range numFields {
			field, err := tuple.GetField(i)
			if err != nil || field == nil {
				row[i] = "NULL"
			} else {
				row[i] = field.String()
			}
		}
		rows = append(rows, row)
	}

	return QueryResult{
		Success: true,
		Columns: columns,
		Rows:    rows,
		Message: fmt.Sprintf("%d row(s) returned", len(rows)),
	}
}

func (db *Database) formatDMLResult(result *planner.DMLResult, stmtType statements.StatementType) QueryResult {
	action := ""
	switch stmtType {
	case statements.Insert:
		action = "inserted"
	case statements.Update:
		action = "updated"
	case statements.Delete:
		action = "deleted"
	}

	return QueryResult{
		Success:      true,
		RowsAffected: result.RowsAffected,
		Message:      fmt.Sprintf("%d row(s) %s", result.RowsAffected, action),
	}
}

func (db *Database) formatDDLResult(result *planner.DDLResult) QueryResult {
	return QueryResult{
		Success: result.Success,
		Message: result.Message,
	}
}

// GetTables returns a list of all tables in the database
func (db *Database) GetTables() []string {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	return db.tableManager.GetAllTableNames()
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
