package plan

import (
	"fmt"
	"storemy/pkg/primitives"
	"strings"
)

// FilterNode represents a selection (WHERE clause filter)
type FilterNode struct {
	BasePlanNode
	Child      PlanNode        // Input relation
	Predicates []PredicateInfo // Filter predicates

	// Parser compatibility fields - for simple filter representation
	Table     string                // Table name (parser usage)
	Field     string                // Field name (parser usage)
	Predicate primitives.Predicate  // Predicate operator (parser usage)
	Constant  string                // Constant value (parser usage)
}

// NewFilterNode creates a new simple filter node for parser usage.
// This is compatible with the parser's WHERE clause representation.
func NewFilterNode(table, field string, predicate primitives.Predicate, constant string) *FilterNode {
	return &FilterNode{
		Table:     table,
		Field:     field,
		Predicate: predicate,
		Constant:  constant,
		Predicates: []PredicateInfo{
			{
				Column:    field,
				Predicate: predicate,
				Value:     constant,
				Type:      StandardPredicate,
			},
		},
	}
}

func (f *FilterNode) GetNodeType() string {
	return "Filter"
}

func (f *FilterNode) String() string {
	// Simple format for parser usage (when no child is set)
	if f.Child == nil && f.Field != "" {
		return fmt.Sprintf("Filter[%s.%s %s %s]", f.Table, f.Field, f.Predicate, f.Constant)
	}

	// Full format for optimizer usage
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Filter(predicates=%d, cost=%.2f, rows=%d)\n",
		len(f.Predicates), f.Cost, f.Cardinality))
	if f.Child != nil {
		sb.WriteString(indent(f.Child.String(), 2))
	}
	return sb.String()
}

func (f *FilterNode) GetChildren() []PlanNode {
	return []PlanNode{f.Child}
}

// ProjectNode represents a projection (SELECT column list)
type ProjectNode struct {
	BasePlanNode
	Child       PlanNode // Input relation
	Columns     []string // Projection columns
	ColumnNames []string // Output column names
}

func (p *ProjectNode) GetNodeType() string {
	return "Project"
}

func (p *ProjectNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Project(columns=%d, cost=%.2f, rows=%d)\n",
		len(p.Columns), p.Cost, p.Cardinality))
	sb.WriteString(indent(p.Child.String(), 2))
	return sb.String()
}

func (p *ProjectNode) GetChildren() []PlanNode {
	return []PlanNode{p.Child}
}

// AggregateNode represents an aggregation operation (GROUP BY)
type AggregateNode struct {
	BasePlanNode
	Child        PlanNode // Input relation
	GroupByExprs []string // GROUP BY expressions
	AggFunctions []string // Aggregate functions (COUNT, SUM, etc.)
}

func (a *AggregateNode) GetNodeType() string {
	return "Aggregate"
}

func (a *AggregateNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Aggregate(groups=%d, aggs=%d, cost=%.2f, rows=%d)\n",
		len(a.GroupByExprs), len(a.AggFunctions), a.Cost, a.Cardinality))
	sb.WriteString(indent(a.Child.String(), 2))
	return sb.String()
}

func (a *AggregateNode) GetChildren() []PlanNode {
	return []PlanNode{a.Child}
}

// SortNode represents a sort operation (ORDER BY)
type SortNode struct {
	BasePlanNode
	Child      PlanNode // Input relation
	SortKey    string   // Sort key (for single column sorts)
	SortKeys   []string // Sort key expressions (for multi-column sorts)
	Directions []string // "ASC" or "DESC" for each key
	Ascending  bool     // True for ASC, false for DESC (for single column)
	Order      string   // "ASC" or "DESC" (for single column)
}

func (s *SortNode) GetNodeType() string {
	return "Sort"
}

func (s *SortNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Sort(keys=%d, cost=%.2f, rows=%d)\n",
		len(s.SortKeys), s.Cost, s.Cardinality))
	sb.WriteString(indent(s.Child.String(), 2))
	return sb.String()
}

func (s *SortNode) GetChildren() []PlanNode {
	return []PlanNode{s.Child}
}

// LimitNode represents a LIMIT/OFFSET operation
type LimitNode struct {
	BasePlanNode
	Child  PlanNode // Input relation
	Limit  int      // Maximum rows to return
	Offset int      // Number of rows to skip
}

func (l *LimitNode) GetNodeType() string {
	return "Limit"
}

func (l *LimitNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Limit(limit=%d, offset=%d, cost=%.2f, rows=%d)\n",
		l.Limit, l.Offset, l.Cost, l.Cardinality))
	sb.WriteString(indent(l.Child.String(), 2))
	return sb.String()
}

func (l *LimitNode) GetChildren() []PlanNode {
	return []PlanNode{l.Child}
}

// MaterializeNode represents materialization of intermediate results
type MaterializeNode struct {
	BasePlanNode
	Child PlanNode // Input relation to materialize
}

func (m *MaterializeNode) GetNodeType() string {
	return "Materialize"
}

func (m *MaterializeNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Materialize(cost=%.2f, rows=%d)\n", m.Cost, m.Cardinality))
	sb.WriteString(indent(m.Child.String(), 2))
	return sb.String()
}

func (m *MaterializeNode) GetChildren() []PlanNode {
	return []PlanNode{m.Child}
}

// DistinctNode represents a DISTINCT operation (duplicate elimination)
type DistinctNode struct {
	BasePlanNode
	Child         PlanNode // Input relation
	DistinctExprs []string // Columns to consider for distinctness (empty = all columns)
}

func (d *DistinctNode) GetNodeType() string {
	return "Distinct"
}

func (d *DistinctNode) String() string {
	var sb strings.Builder
	if len(d.DistinctExprs) > 0 {
		sb.WriteString(fmt.Sprintf("Distinct(columns=%d, cost=%.2f, rows=%d)\n",
			len(d.DistinctExprs), d.Cost, d.Cardinality))
	} else {
		sb.WriteString(fmt.Sprintf("Distinct(all columns, cost=%.2f, rows=%d)\n",
			d.Cost, d.Cardinality))
	}
	sb.WriteString(indent(d.Child.String(), 2))
	return sb.String()
}

func (d *DistinctNode) GetChildren() []PlanNode {
	return []PlanNode{d.Child}
}

// SetOpNode represents a set operation (UNION, INTERSECT, EXCEPT)
type SetOpNode struct {
	BasePlanNode
	LeftChild  PlanNode // Left input relation
	RightChild PlanNode // Right input relation
	OpType     string   // "UNION", "INTERSECT", "EXCEPT", etc.
}

func (s *SetOpNode) GetNodeType() string {
	return "SetOp"
}

func (s *SetOpNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("SetOp(type=%s, cost=%.2f, rows=%d)\n",
		s.OpType, s.Cost, s.Cardinality))
	sb.WriteString(indent(s.LeftChild.String(), 2))
	sb.WriteString(indent(s.RightChild.String(), 2))
	return sb.String()
}

func (s *SetOpNode) GetChildren() []PlanNode {
	return []PlanNode{s.LeftChild, s.RightChild}
}

// InsertNode represents an INSERT operation
type InsertNode struct {
	BasePlanNode
	TableName string // Table to insert into
	NumRows   int    // Number of rows to insert
}

func (i *InsertNode) GetNodeType() string {
	return "Insert"
}

func (i *InsertNode) String() string {
	return fmt.Sprintf("Insert(table=%s, rows=%d, cost=%.2f)\n",
		i.TableName, i.NumRows, i.Cost)
}

func (i *InsertNode) GetChildren() []PlanNode {
	return []PlanNode{}
}

// UpdateNode represents an UPDATE operation
type UpdateNode struct {
	BasePlanNode
	Child     PlanNode // Input relation (scanned rows to update)
	TableName string   // Table to update
	SetFields int      // Number of fields being set
}

func (u *UpdateNode) GetNodeType() string {
	return "Update"
}

func (u *UpdateNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Update(table=%s, fields=%d, cost=%.2f, rows=%d)\n",
		u.TableName, u.SetFields, u.Cost, u.Cardinality))
	sb.WriteString(indent(u.Child.String(), 2))
	return sb.String()
}

func (u *UpdateNode) GetChildren() []PlanNode {
	return []PlanNode{u.Child}
}

// DeleteNode represents a DELETE operation
type DeleteNode struct {
	BasePlanNode
	Child     PlanNode // Input relation (scanned rows to delete)
	TableName string   // Table to delete from
}

func (d *DeleteNode) GetNodeType() string {
	return "Delete"
}

func (d *DeleteNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Delete(table=%s, cost=%.2f, rows=%d)\n",
		d.TableName, d.Cost, d.Cardinality))
	sb.WriteString(indent(d.Child.String(), 2))
	return sb.String()
}

func (d *DeleteNode) GetChildren() []PlanNode {
	return []PlanNode{d.Child}
}

// DDLNode represents a DDL operation (CREATE, DROP, ALTER)
type DDLNode struct {
	BasePlanNode
	Operation  string // "CREATE TABLE", "DROP INDEX", etc.
	ObjectName string // Name of object being created/dropped/altered
}

func (d *DDLNode) GetNodeType() string {
	return "DDL"
}

func (d *DDLNode) String() string {
	return fmt.Sprintf("DDL(operation=%s, object=%s, cost=%.2f)\n",
		d.Operation, d.ObjectName, d.Cost)
}

func (d *DDLNode) GetChildren() []PlanNode {
	return []PlanNode{}
}
