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
	SortKeys   []string // Sort key expressions
	Directions []string // "ASC" or "DESC" for each key
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
