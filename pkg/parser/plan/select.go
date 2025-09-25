package plan

import (
	"errors"
	"fmt"
	"storemy/pkg/types"
	"strings"
)

type SelectListNode struct {
	FieldName string // Field name (may be qualified: table.field)
	AggOp     string // Aggregation operation (SUM, COUNT, etc.) or empty string
}

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

type ScanNode struct {
	TableName string // Name of the table in the catalog
	Alias     string // Table alias (e.g., "u" for "users u")
}

func NewScanNode(tableName, alias string) *ScanNode {
	return &ScanNode{
		TableName: tableName,
		Alias:     alias,
	}
}

func (sn *ScanNode) String() string {
	return fmt.Sprintf("Scan[table=%s, alias=%s]", sn.TableName, sn.Alias)
}

type SelectPlan struct {
	groupByField string // GROUP BY field (only one supported)
	hasAgg       bool   // Whether query has aggregation
	aggOp        string // Aggregation operation
	aggField     string // Field being aggregated
	hasOrderBy   bool   // Whether query has ORDER BY
	orderByAsc   bool   // ORDER BY direction
	orderByField string // ORDER BY field
	query        string // Original SQL query string

	filters    []*FilterNode     // All WHERE conditions
	selectList []*SelectListNode // SELECT clause items
	tables     []*ScanNode       // All table scans
}

func NewSelectPlan() *SelectPlan {
	return &SelectPlan{}
}

func (sp *SelectPlan) String() string {
	return sp.query
}

func (sp *SelectPlan) AddProjectField(fieldName, aggOp string) error {
	selectNode := NewSelectListNode(fieldName, aggOp)
	sp.selectList = append(sp.selectList, selectNode)

	if aggOp != "" {
		sp.hasAgg = true
		sp.aggOp = aggOp
		sp.aggField = fieldName
	}

	return nil
}

func (sp *SelectPlan) SetGroupBy(fieldName string) {
	sp.groupByField = fieldName
}

func (sp *SelectPlan) AddOrderBy(field string, ascending bool) {
	sp.hasOrderBy = true
	sp.orderByField = field
	sp.orderByAsc = ascending
}

func (sp *SelectPlan) AddScan(tableName string, alias string) {
	fmt.Printf("Added scan of table %s\n", alias)
	scan := NewScanNode(tableName, alias)
	sp.tables = append(sp.tables, scan)
}

func (sp *SelectPlan) AddFilter(field string, pred types.Predicate, constant string) error {
	parts := strings.Split(field, ".")
	if len(parts) != 2 {
		return errors.New("field name must be fully qualified for filters")
	}

	table := parts[0]
	filter := NewFilterNode(table, field, pred, constant)
	fmt.Printf("Added filter %s %s %s\n", field, pred, constant)
	sp.filters = append(sp.filters, filter)
	return nil
}
