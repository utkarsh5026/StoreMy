package plan

import (
	"fmt"
	"strings"
)

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

// NewScanNode creates a new simple table scan node with the given table name and alias.
// This is a convenience constructor for parser usage.
func NewScanNode(tableName, alias string) *ScanNode {
	return &ScanNode{
		TableName:    tableName,
		Alias:        alias,
		AccessMethod: "seqscan", // Default to sequential scan
	}
}

func (s *ScanNode) GetNodeType() string {
	return "Scan"
}

func (s *ScanNode) String() string {
	// Simple format for parser usage (no cost/cardinality)
	if s.Cost == 0 && s.Cardinality == 0 {
		return fmt.Sprintf("Scan[table=%s, alias=%s]", s.TableName, s.Alias)
	}

	// Full format for optimizer usage
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
