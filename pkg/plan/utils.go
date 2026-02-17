package plan

import (
	"fmt"
	"strings"
)

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
	switch {
	case prefix == "":
		sb.WriteString("") // Root node
	case isLast:
		sb.WriteString(prefix + "└── ")
	default:
		sb.WriteString(prefix + "├── ")
	}

	// Node description
	sb.WriteString(fmt.Sprintf("%s [cost=%.2f, rows=%d]\n",
		node.GetNodeType(), node.GetCost(), node.GetCardinality()))

	// Children
	children := node.GetChildren()
	for i, child := range children {
		childPrefix := prefix
		switch {
		case prefix == "":
			childPrefix = ""
		case isLast:
			childPrefix += "    "
		default:
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
