package result

import (
	"fmt"
	"storemy/pkg/tuple"
)

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
