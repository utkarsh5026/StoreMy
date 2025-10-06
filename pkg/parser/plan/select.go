package plan

import (
	"errors"
	"fmt"
	"storemy/pkg/types"
	"strings"
)

// SelectPlan represents the execution plan for a SELECT query.
// It contains all the parsed components of a SELECT statement including
// projections, filters, joins, aggregations, and ordering.
type SelectPlan struct {
	selectList []*SelectListNode
	selectAll  bool

	tables []*ScanNode
	joins  []*JoinNode

	filters []*FilterNode

	hasAgg       bool
	aggOp        string
	aggField     string
	groupByField string

	hasOrderBy   bool
	orderByField string
	orderByAsc   bool

	query string
}

// NewSelectPlan creates a new SelectPlan with default values.
func NewSelectPlan() *SelectPlan {
	return &SelectPlan{
		selectList: make([]*SelectListNode, 0),
		tables:     make([]*ScanNode, 0),
		joins:      make([]*JoinNode, 0),
		filters:    make([]*FilterNode, 0),
	}
}

func (sp *SelectPlan) String() string {
	return sp.query
}

// AddProjectField adds a field to the SELECT clause, with optional aggregation.
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

// SetGroupBy sets the GROUP BY field for the query.
func (sp *SelectPlan) SetGroupBy(fieldName string) {
	sp.groupByField = fieldName
}

// AddOrderBy adds an ORDER BY clause to the query.
func (sp *SelectPlan) AddOrderBy(field string, ascending bool) {
	sp.hasOrderBy = true
	sp.orderByField = field
	sp.orderByAsc = ascending
}

// AddScan adds a table scan to the FROM clause.
func (sp *SelectPlan) AddScan(tableName string, alias string) {
	fmt.Printf("Added scan of table %s\n", alias)
	scan := NewScanNode(tableName, alias)
	sp.tables = append(sp.tables, scan)
}

// AddFilter adds a WHERE clause filter condition.
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

// AddJoin adds a JOIN clause to the query.
func (sp *SelectPlan) AddJoin(rightTable *ScanNode, joinType JoinType, leftField, rightField string, predicate types.Predicate) {
	join := NewJoinNode(rightTable, joinType, leftField, rightField, predicate)
	sp.joins = append(sp.joins, join)
	fmt.Printf("Added join: %s\n", join.String())
}

func (sp *SelectPlan) SelectList() []*SelectListNode {
	return sp.selectList
}

func (sp *SelectPlan) SelectAll() bool {
	return sp.selectAll
}

func (sp *SelectPlan) SetSelectAll(selectAll bool) {
	sp.selectAll = selectAll
}

func (sp *SelectPlan) Tables() []*ScanNode {
	return sp.tables
}

func (sp *SelectPlan) Joins() []*JoinNode {
	return sp.joins
}

func (sp *SelectPlan) Filters() []*FilterNode {
	return sp.filters
}

func (sp *SelectPlan) HasAgg() bool {
	return sp.hasAgg
}

func (sp *SelectPlan) AggOp() string {
	return sp.aggOp
}

func (sp *SelectPlan) AggField() string {
	return sp.aggField
}

func (sp *SelectPlan) GroupByField() string {
	return sp.groupByField
}

func (sp *SelectPlan) HasOrderBy() bool {
	return sp.hasOrderBy
}

func (sp *SelectPlan) OrderByField() string {
	return sp.orderByField
}

func (sp *SelectPlan) OrderByAsc() bool {
	return sp.orderByAsc
}
