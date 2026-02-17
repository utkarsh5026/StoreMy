package plan

import (
	"fmt"
	"storemy/pkg/primitives"
	"strings"
)

// JoinNode represents a join operation between two relations
type JoinNode struct {
	BasePlanNode
	LeftChild     PlanNode             // Left input relation
	RightChild    PlanNode             // Right input relation
	JoinType      string               // "inner", "left", "right", "full" (also supports JoinType enum via helpers)
	JoinMethod    string               // "hash", "merge", "nested", "index"
	LeftColumn    string               // Left join column (also LeftField for parser compatibility)
	RightColumn   string               // Right join column (also RightField for parser compatibility)
	JoinPredicate primitives.Predicate // Join predicate operator (also Predicate for parser compatibility)
	ExtraFilters  []PredicateInfo      // Additional filters applied after join

	// Parser compatibility fields
	RightTable *ScanNode            // For parser usage - references the right table scan
	LeftField  string               // Alias for LeftColumn (parser compatibility)
	RightField string               // Alias for RightColumn (parser compatibility)
	Predicate  primitives.Predicate // Alias for JoinPredicate (parser compatibility)
}

// NewJoinNode creates a new join node for parser usage.
// This constructor is compatible with the parser's simple join representation.
func NewJoinNode(rightTable *ScanNode, joinType JoinType, leftField, rightField string, predicate primitives.Predicate) *JoinNode {
	joinTypeStr := ""
	switch joinType {
	case InnerJoin:
		joinTypeStr = "inner"
	case LeftJoin:
		joinTypeStr = "left"
	case RightJoin:
		joinTypeStr = "right"
	case CrossJoin:
		joinTypeStr = "cross"
	}

	return &JoinNode{
		RightTable:    rightTable,
		RightChild:    rightTable, // Set RightChild to the scan node for compatibility
		JoinType:      joinTypeStr,
		LeftColumn:    leftField,
		RightColumn:   rightField,
		LeftField:     leftField,
		RightField:    rightField,
		JoinPredicate: predicate,
		Predicate:     predicate,
		JoinMethod:    "hash", // Default join method
	}
}

func (j *JoinNode) GetNodeType() string {
	return "Join"
}

func (j *JoinNode) String() string {
	// Simple format for parser usage (when RightTable is set but LeftChild/RightChild are not full trees)
	if j.RightTable != nil && j.LeftChild == nil {
		return fmt.Sprintf("%s JOIN %s ON %s = %s", j.JoinType, j.RightTable.TableName, j.LeftField, j.RightField)
	}

	// Full format for optimizer usage
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Join(%s %s", j.JoinType, j.JoinMethod))
	if j.LeftColumn != "" && j.RightColumn != "" {
		sb.WriteString(fmt.Sprintf(", on=%s=%s", j.LeftColumn, j.RightColumn))
	}
	if len(j.ExtraFilters) > 0 {
		sb.WriteString(fmt.Sprintf(", filters=%d", len(j.ExtraFilters)))
	}
	sb.WriteString(fmt.Sprintf(", cost=%.2f, rows=%d)\n", j.Cost, j.Cardinality))

	if j.LeftChild != nil {
		sb.WriteString(indent(j.LeftChild.String(), 2))
		sb.WriteString("\n")
	}
	if j.RightChild != nil {
		sb.WriteString(indent(j.RightChild.String(), 2))
	}
	return sb.String()
}

func (j *JoinNode) GetChildren() []PlanNode {
	return []PlanNode{j.LeftChild, j.RightChild}
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

// IntersectNode represents an INTERSECT operation
type IntersectNode struct {
	BasePlanNode
	LeftChild    PlanNode // Left input relation
	RightChild   PlanNode // Right input relation
	IntersectAll bool     // true for INTERSECT ALL, false for INTERSECT (with deduplication)
}

func (i *IntersectNode) GetNodeType() string {
	return "Intersect"
}

func (i *IntersectNode) String() string {
	var sb strings.Builder
	intersectType := "INTERSECT"
	if i.IntersectAll {
		intersectType = "INTERSECT ALL"
	}
	sb.WriteString(fmt.Sprintf("Intersect(%s, cost=%.2f, rows=%d)\n", intersectType, i.Cost, i.Cardinality))
	sb.WriteString(indent(i.LeftChild.String(), 2))
	sb.WriteString("\n")
	sb.WriteString(indent(i.RightChild.String(), 2))
	return sb.String()
}

func (i *IntersectNode) GetChildren() []PlanNode {
	return []PlanNode{i.LeftChild, i.RightChild}
}

// ExceptNode represents an EXCEPT (MINUS) operation
type ExceptNode struct {
	BasePlanNode
	LeftChild  PlanNode // Left input relation
	RightChild PlanNode // Right input relation
	ExceptAll  bool     // true for EXCEPT ALL, false for EXCEPT (with deduplication)
}

func (e *ExceptNode) GetNodeType() string {
	return "Except"
}

func (e *ExceptNode) String() string {
	var sb strings.Builder
	exceptType := "EXCEPT"
	if e.ExceptAll {
		exceptType = "EXCEPT ALL"
	}
	sb.WriteString(fmt.Sprintf("Except(%s, cost=%.2f, rows=%d)\n", exceptType, e.Cost, e.Cardinality))
	sb.WriteString(indent(e.LeftChild.String(), 2))
	sb.WriteString("\n")
	sb.WriteString(indent(e.RightChild.String(), 2))
	return sb.String()
}

func (e *ExceptNode) GetChildren() []PlanNode {
	return []PlanNode{e.LeftChild, e.RightChild}
}
