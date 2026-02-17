package plan

import (
	"fmt"
	"storemy/pkg/primitives"
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

// PredicateType represents the type of predicate for specialized handling
type PredicateType int

const (
	// StandardPredicate represents comparison operators (=, <, >, <=, >=, !=, <>)
	StandardPredicate PredicateType = iota
	// NullCheckPredicate represents IS NULL or IS NOT NULL
	NullCheckPredicate
	// LikePredicate represents LIKE pattern matching
	LikePredicate
	// InPredicate represents IN (...) clause with multiple values
	InPredicate
)

// PredicateInfo represents a filter predicate applied in scan or filter nodes.
// It supports different types of predicates with their specific requirements.
type PredicateInfo struct {
	Column    string               // Column name being filtered
	Predicate primitives.Predicate // Predicate operator (=, <, >, LIKE, etc.)
	Value     string               // Primary value (for standard predicates and LIKE patterns)
	Values    []string             // Multiple values (for IN predicates)
	Type      PredicateType        // Type of predicate for specialized handling
	IsNull    bool                 // For null checks: true for IS NULL, false for IS NOT NULL
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

// SelectListNode represents a field or expression in the SELECT clause.
// It can be a simple field or an aggregated field (e.g., COUNT, SUM).
type SelectListNode struct {
	FieldName string
	AggOp     string
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

// SetOperationType defines the type of set operation
type SetOperationType int

const (
	UnionOp SetOperationType = iota
	IntersectOp
	ExceptOp
)
