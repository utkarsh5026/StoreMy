package plan

import (
	"fmt"
	"storemy/pkg/types"
)

// ScanNode represents a table scan in the query plan.
// It contains the table name and optional alias for identification.
type ScanNode struct {
	TableName string // Name of the table in the catalog
	Alias     string // Table alias (e.g., "u" for "users u")
}

// NewScanNode creates a new table scan node with the given table name and alias.
func NewScanNode(tableName, alias string) *ScanNode {
	return &ScanNode{
		TableName: tableName,
		Alias:     alias,
	}
}

func (sn *ScanNode) String() string {
	return fmt.Sprintf("Scan[table=%s, alias=%s]", sn.TableName, sn.Alias)
}

// JoinType represents the type of join operation
type JoinType int

const (
	CrossJoin JoinType = iota
	InnerJoin
	LeftJoin
	RightJoin
)

func (jt JoinType) String() string {
	switch jt {
	case CrossJoin:
		return "CROSS"
	case InnerJoin:
		return "INNER"
	case LeftJoin:
		return "LEFT"
	case RightJoin:
		return "RIGHT"
	default:
		return "UNKNOWN"
	}
}

// JoinNode represents a join operation between two tables.
// It specifies the join type, tables involved, and join condition.
type JoinNode struct {
	RightTable *ScanNode      // Table to join
	JoinType   JoinType       // Type of join
	LeftField  string         // Left join field (table.field)
	RightField string         // Right join field (table.field)
	Predicate  types.Predicate // Join condition predicate
}

// NewJoinNode creates a new join node with the specified parameters.
func NewJoinNode(rightTable *ScanNode, joinType JoinType, leftField, rightField string, predicate types.Predicate) *JoinNode {
	return &JoinNode{
		RightTable: rightTable,
		JoinType:   joinType,
		LeftField:  leftField,
		RightField: rightField,
		Predicate:  predicate,
	}
}

func (jn *JoinNode) String() string {
	return fmt.Sprintf("%s JOIN %s ON %s = %s", jn.JoinType, jn.RightTable.TableName, jn.LeftField, jn.RightField)
}

// FilterNode represents a WHERE clause filter condition.
// It specifies which field to filter, the comparison operator, and the constant value.
type FilterNode struct {
	Table     string          // Table alias the filter applies to
	Field     string          // Field name being filtered
	Predicate types.Predicate // Comparison operator
	Constant  string          // Constant value to compare against
}

// NewFilterNode creates a new filter node with the specified parameters.
func NewFilterNode(table, field string, predicate types.Predicate, constant string) *FilterNode {
	return &FilterNode{
		Table:     table,
		Field:     field,
		Predicate: predicate,
		Constant:  constant,
	}
}

func (fn *FilterNode) String() string {
	return fmt.Sprintf("Filter[%s.%s %s %s]", fn.Table, fn.Field, fn.Predicate, fn.Constant)
}

// SelectListNode represents a field or expression in the SELECT clause.
// It can be a simple field or an aggregated field (e.g., COUNT, SUM).
type SelectListNode struct {
	FieldName string // Field name (may be qualified: table.field)
	AggOp     string // Aggregation operation (SUM, COUNT, etc.) or empty string
}

// NewSelectListNode creates a new select list node for a field with optional aggregation.
func NewSelectListNode(fieldName, aggOp string) *SelectListNode {
	return &SelectListNode{
		FieldName: fieldName,
		AggOp:     aggOp,
	}
}

func (sln *SelectListNode) String() string {
	if sln.AggOp != "" {
		return fmt.Sprintf("%s(%s)", sln.AggOp, sln.FieldName)
	}
	return sln.FieldName
}
