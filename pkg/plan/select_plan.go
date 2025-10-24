package plan

import (
	"fmt"
	"storemy/pkg/primitives"
	"strings"
)

// SelectPlan represents the execution plan for a SELECT query.
// It contains all the parsed components of a SELECT statement including
// projections, filters, joins, aggregations, ordering, and DISTINCT.
// It can also represent a set operation (UNION, INTERSECT, EXCEPT) combining two SELECT queries.
type SelectPlan struct {
	selectList []*SelectListNode
	selectAll  bool
	distinct   bool // true for SELECT DISTINCT

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

	// Set operation fields
	isSetOperation bool
	setOpType      SetOperationType
	setOpAll       bool // true for UNION ALL, INTERSECT ALL, EXCEPT ALL
	leftPlan       *SelectPlan
	rightPlan      *SelectPlan

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
	scan := NewScanNode(tableName, alias)
	sp.tables = append(sp.tables, scan)
}

// AddFilter adds a WHERE clause filter condition.
// Accepts both qualified (table.column) and unqualified (column) field names.
// For unqualified names, infers the table from the FROM clause if possible.
func (sp *SelectPlan) AddFilter(field string, pred primitives.Predicate, constant string) error {
	var table string
	var qualifiedField string

	parts := strings.Split(field, ".")
	if len(parts) == 2 {
		table = parts[0]
		qualifiedField = field
	} else if len(parts) == 1 {
		if len(sp.tables) > 0 {
			// Use the first table's alias if available, otherwise use table name
			if sp.tables[0].Alias != "" {
				table = sp.tables[0].Alias
			} else {
				table = sp.tables[0].TableName
			}
			qualifiedField = table + "." + field
		} else {
			table = ""
			qualifiedField = field
		}
	} else {
		return fmt.Errorf("invalid field name format: %s", field)
	}

	filter := NewFilterNode(table, qualifiedField, pred, constant)
	sp.filters = append(sp.filters, filter)
	return nil
}

// AddJoin adds a JOIN clause to the query.
func (sp *SelectPlan) AddJoin(rightTable *ScanNode, joinType JoinType, leftField, rightField string, predicate primitives.Predicate) {
	join := NewJoinNode(rightTable, joinType, leftField, rightField, predicate)
	sp.joins = append(sp.joins, join)
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

// NewSetOperationPlan creates a SelectPlan that represents a set operation.
func NewSetOperationPlan(left, right *SelectPlan, opType SetOperationType, isAll bool) *SelectPlan {
	return &SelectPlan{
		isSetOperation: true,
		setOpType:      opType,
		setOpAll:       isAll,
		leftPlan:       left,
		rightPlan:      right,
	}
}

// IsSetOperation returns true if this plan represents a set operation.
func (sp *SelectPlan) IsSetOperation() bool {
	return sp.isSetOperation
}

// SetOpType returns the type of set operation (UNION, INTERSECT, EXCEPT).
func (sp *SelectPlan) SetOpType() SetOperationType {
	return sp.setOpType
}

// SetOpAll returns true for UNION ALL, INTERSECT ALL, EXCEPT ALL.
func (sp *SelectPlan) SetOpAll() bool {
	return sp.setOpAll
}

// LeftPlan returns the left child plan for set operations.
func (sp *SelectPlan) LeftPlan() *SelectPlan {
	return sp.leftPlan
}

// RightPlan returns the right child plan for set operations.
func (sp *SelectPlan) RightPlan() *SelectPlan {
	return sp.rightPlan
}

// SetDistinct sets whether this is a SELECT DISTINCT query.
func (sp *SelectPlan) SetDistinct(distinct bool) {
	sp.distinct = distinct
}

// IsDistinct returns true if this is a SELECT DISTINCT query.
func (sp *SelectPlan) IsDistinct() bool {
	return sp.distinct
}
