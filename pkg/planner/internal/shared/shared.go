package shared

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
)

// ---- Result types (previously in internal/result/) ----

// Result represents the outcome of executing a query plan.
// Different plan types return different concrete implementations.
type Result interface {
	// String returns a human-readable representation of the result
	String() string
}

// ResultType categorizes the different kinds of query results
type ResultType int

const (
	DDLResultType ResultType = iota
	DMLResultType
	SelectResultType
	ExplainResultType
)

// DDLResult represents the outcome of DDL operations (CREATE, DROP, ALTER).
type DDLResult struct {
	Success bool
	Message string
}

func (r *DDLResult) String() string {
	return fmt.Sprintf("DDL Result - Success: %t, Message: %s", r.Success, r.Message)
}

func (r *DDLResult) Type() ResultType {
	return DDLResultType
}

// DMLResult represents the outcome of DML operations (INSERT, UPDATE, DELETE).
type DMLResult struct {
	RowsAffected int
	Message      string
}

func (d *DMLResult) String() string {
	return fmt.Sprintf("%d row(s) affected: %s", d.RowsAffected, d.Message)
}

func (d *DMLResult) Type() ResultType {
	return DMLResultType
}

// SelectQueryResult represents the outcome of SELECT queries.
type SelectQueryResult struct {
	TupleDesc *tuple.TupleDescription
	Tuples    []*tuple.Tuple
}

func (r *SelectQueryResult) String() string {
	return fmt.Sprintf("Query returned %d row(s)", len(r.Tuples))
}

func (r *SelectQueryResult) Type() ResultType {
	return SelectResultType
}

// ExplainResult represents the outcome of EXPLAIN queries.
type ExplainResult struct {
	Plan    string // The query execution plan as text
	Format  string // The format used (TEXT, JSON, etc.)
	Analyze bool   // Whether this was an EXPLAIN ANALYZE
}

func (r *ExplainResult) String() string {
	return r.Plan
}

func (r *ExplainResult) Type() ResultType {
	return ExplainResultType
}

// NewDDLResult creates a new DDL result with the given success status and message.
func NewDDLResult(success bool, message string) *DDLResult {
	return &DDLResult{
		Success: success,
		Message: message,
	}
}

// NewDMLResult creates a new DML result with the given rows affected and message.
func NewDMLResult(rowsAffected int, message string) *DMLResult {
	return &DMLResult{
		RowsAffected: rowsAffected,
		Message:      message,
	}
}

// NewSelectQueryResult creates a new SELECT query result with the given tuple description and tuples.
func NewSelectQueryResult(tupleDesc *tuple.TupleDescription, tuples []*tuple.Tuple) *SelectQueryResult {
	return &SelectQueryResult{
		TupleDesc: tupleDesc,
		Tuples:    tuples,
	}
}

// NewExplainResult creates a new EXPLAIN result with the given plan, format, and analyze flag.
func NewExplainResult(plan, format string, analyze bool) *ExplainResult {
	return &ExplainResult{
		Plan:    plan,
		Format:  format,
		Analyze: analyze,
	}
}

// ---- Table metadata helpers (previously in internal/metadata/) ----

type TableMetadata struct {
	TableID   primitives.FileID
	TupleDesc *tuple.TupleDescription
}

// ResolveTableMetadata retrieves table ID and schema in a single operation.
// This is the primary table lookup method used by all planner components.
func ResolveTableMetadata(tableName string, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) (*TableMetadata, error) {
	catalogMgr := ctx.CatalogManager()
	tableID, err := catalogMgr.GetTableID(tx, tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	sch, err := catalogMgr.GetTableSchema(tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %s: %v", tableName, err)
	}

	return &TableMetadata{
		TableID:   tableID,
		TupleDesc: sch.TupleDesc,
	}, nil
}

// ResolveTableID converts a table name to its internal numeric identifier.
// Convenience wrapper around ResolveTableMetadata when only the ID is needed.
func ResolveTableID(tableName string, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) (primitives.FileID, error) {
	md, err := ResolveTableMetadata(tableName, tx, ctx)
	if err != nil {
		return 0, err
	}

	return md.TableID, nil
}

// CollectAllTuples executes an iterator and materializes all results into memory.
// This is used by operators that require full result sets (e.g., ORDER BY, aggregation).
func CollectAllTuples(it iterator.DbIterator) ([]*tuple.Tuple, error) {
	if err := it.Open(); err != nil {
		return nil, fmt.Errorf("failed to open iterator: %v", err)
	}
	defer it.Close()
	return iterator.Map(it, func(t *tuple.Tuple) (*tuple.Tuple, error) {
		return t, nil
	})
}
