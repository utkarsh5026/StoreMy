package planner

import (
	"fmt"
	"storemy/pkg/primitives"
	"strings"
)

// PlanNode represents a node in the query execution plan tree
// This is the logical representation before physical execution operators are created
type PlanNode interface {
	// GetCost returns the estimated total cost of executing this node and its children
	GetCost() float64

	// GetCardinality returns the estimated number of rows this node will produce
	GetCardinality() int64

	// GetChildren returns the child plan nodes
	GetChildren() []PlanNode

	// GetNodeType returns the type of this node (for debugging/visualization)
	GetNodeType() string

	// String returns a human-readable representation of the plan
	String() string

	// SetCost sets the estimated cost (used by optimizer)
	SetCost(cost float64)

	// SetCardinality sets the estimated cardinality (used by optimizer)
	SetCardinality(card int64)
}

// BasePlanNode provides common functionality for all plan nodes
type BasePlanNode struct {
	Cost        float64
	Cardinality int64
	Children    []PlanNode
}

func (b *BasePlanNode) GetCost() float64 {
	return b.Cost
}

func (b *BasePlanNode) GetCardinality() int64 {
	return b.Cardinality
}

func (b *BasePlanNode) GetChildren() []PlanNode {
	return b.Children
}

func (b *BasePlanNode) SetCost(cost float64) {
	b.Cost = cost
}

func (b *BasePlanNode) SetCardinality(card int64) {
	b.Cardinality = card
}

// PredicateInfo represents a filter predicate applied in scan or filter nodes
type PredicateInfo struct {
	Column    string
	Predicate primitives.Predicate
	Value     string
}

// ScanNode represents a table scan operation
type ScanNode struct {
	BasePlanNode
	TableName    string          // Name of the table being scanned
	TableID      int             // Table identifier
	AccessMethod string          // "seqscan", "indexscan", "indexonlyscan"
	IndexName    string          // Index name (if using index scan)
	IndexID      int             // Index ID (if using index scan)
	Predicates   []PredicateInfo // Filter predicates pushed down to scan
	Alias        string          // Table alias (if any)
}

func (s *ScanNode) GetNodeType() string {
	return "Scan"
}

func (s *ScanNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Scan(%s", s.TableName))
	if s.Alias != "" {
		sb.WriteString(fmt.Sprintf(" AS %s", s.Alias))
	}
	sb.WriteString(fmt.Sprintf(", method=%s", s.AccessMethod))
	if s.IndexName != "" {
		sb.WriteString(fmt.Sprintf(", index=%s", s.IndexName))
	}
	if len(s.Predicates) > 0 {
		sb.WriteString(fmt.Sprintf(", filters=%d", len(s.Predicates)))
	}
	sb.WriteString(fmt.Sprintf(", cost=%.2f, rows=%d)", s.Cost, s.Cardinality))
	return sb.String()
}

// JoinNode represents a join operation between two relations
type JoinNode struct {
	BasePlanNode
	LeftChild     PlanNode             // Left input relation
	RightChild    PlanNode             // Right input relation
	JoinType      string               // "inner", "left", "right", "full"
	JoinMethod    string               // "hash", "merge", "nested", "index"
	LeftColumn    string               // Left join column
	RightColumn   string               // Right join column
	JoinPredicate primitives.Predicate // Join predicate operator
	ExtraFilters  []PredicateInfo      // Additional filters applied after join
}

func (j *JoinNode) GetNodeType() string {
	return "Join"
}

func (j *JoinNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Join(%s %s", j.JoinType, j.JoinMethod))
	if j.LeftColumn != "" && j.RightColumn != "" {
		sb.WriteString(fmt.Sprintf(", on=%s=%s", j.LeftColumn, j.RightColumn))
	}
	if len(j.ExtraFilters) > 0 {
		sb.WriteString(fmt.Sprintf(", filters=%d", len(j.ExtraFilters)))
	}
	sb.WriteString(fmt.Sprintf(", cost=%.2f, rows=%d)\n", j.Cost, j.Cardinality))
	sb.WriteString(indent(j.LeftChild.String(), 2))
	sb.WriteString("\n")
	sb.WriteString(indent(j.RightChild.String(), 2))
	return sb.String()
}

func (j *JoinNode) GetChildren() []PlanNode {
	return []PlanNode{j.LeftChild, j.RightChild}
}

// FilterNode represents a selection (WHERE clause filter)
type FilterNode struct {
	BasePlanNode
	Child      PlanNode        // Input relation
	Predicates []PredicateInfo // Filter predicates
}

func (f *FilterNode) GetNodeType() string {
	return "Filter"
}

func (f *FilterNode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Filter(predicates=%d, cost=%.2f, rows=%d)\n",
		len(f.Predicates), f.Cost, f.Cardinality))
	sb.WriteString(indent(f.Child.String(), 2))
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

// UnionNode represents a UNION operation
type UnionNode struct {
	BasePlanNode
	LeftChild  PlanNode // Left input relation
	RightChild PlanNode // Right input relation
	UnionAll   bool     // true for UNION ALL, false for UNION (with deduplication)
}

func (u *UnionNode) GetNodeType() string {
	return "Union"
}

func (u *UnionNode) String() string {
	var sb strings.Builder
	unionType := "UNION"
	if u.UnionAll {
		unionType = "UNION ALL"
	}
	sb.WriteString(fmt.Sprintf("Union(%s, cost=%.2f, rows=%d)\n", unionType, u.Cost, u.Cardinality))
	sb.WriteString(indent(u.LeftChild.String(), 2))
	sb.WriteString("\n")
	sb.WriteString(indent(u.RightChild.String(), 2))
	return sb.String()
}

func (u *UnionNode) GetChildren() []PlanNode {
	return []PlanNode{u.LeftChild, u.RightChild}
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

// Helper function to indent multi-line strings
func indent(s string, spaces int) string {
	prefix := strings.Repeat(" ", spaces)
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		if line != "" {
			lines[i] = prefix + line
		}
	}
	return strings.Join(lines, "\n")
}

// PlanVisualizer provides methods to visualize query plans
type PlanVisualizer struct{}

// NewPlanVisualizer creates a new plan visualizer
func NewPlanVisualizer() *PlanVisualizer {
	return &PlanVisualizer{}
}

// Visualize returns a tree-like visualization of the plan
func (pv *PlanVisualizer) Visualize(plan PlanNode) string {
	return pv.visualizeNode(plan, "", true)
}

func (pv *PlanVisualizer) visualizeNode(node PlanNode, prefix string, isLast bool) string {
	var sb strings.Builder

	// Current node connector
	if prefix == "" {
		sb.WriteString("") // Root node
	} else if isLast {
		sb.WriteString(prefix + "└── ")
	} else {
		sb.WriteString(prefix + "├── ")
	}

	// Node description
	sb.WriteString(fmt.Sprintf("%s [cost=%.2f, rows=%d]\n",
		node.GetNodeType(), node.GetCost(), node.GetCardinality()))

	// Children
	children := node.GetChildren()
	for i, child := range children {
		childPrefix := prefix
		if prefix == "" {
			childPrefix = ""
		} else if isLast {
			childPrefix += "    "
		} else {
			childPrefix += "│   "
		}
		sb.WriteString(pv.visualizeNode(child, childPrefix, i == len(children)-1))
	}

	return sb.String()
}

// GetPlanCost returns the total cost of a plan tree
func GetPlanCost(plan PlanNode) float64 {
	if plan == nil {
		return 0.0
	}
	return plan.GetCost()
}

// GetPlanCardinality returns the estimated output cardinality
func GetPlanCardinality(plan PlanNode) int64 {
	if plan == nil {
		return 0
	}
	return plan.GetCardinality()
}
