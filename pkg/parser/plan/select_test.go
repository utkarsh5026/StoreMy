package plan

import (
	"storemy/pkg/types"
	"testing"
)

// TestNewSelectPlan tests the SelectPlan constructor
func TestNewSelectPlan(t *testing.T) {
	plan := NewSelectPlan()

	if plan == nil {
		t.Fatal("NewSelectPlan() returned nil")
	}
	if plan.selectList == nil {
		t.Error("selectList should be initialized")
	}
	if plan.tables == nil {
		t.Error("tables should be initialized")
	}
	if plan.joins == nil {
		t.Error("joins should be initialized")
	}
	if plan.filters == nil {
		t.Error("filters should be initialized")
	}
	if len(plan.selectList) != 0 {
		t.Errorf("selectList length = %d, want 0", len(plan.selectList))
	}
	if len(plan.tables) != 0 {
		t.Errorf("tables length = %d, want 0", len(plan.tables))
	}
	if len(plan.joins) != 0 {
		t.Errorf("joins length = %d, want 0", len(plan.joins))
	}
	if len(plan.filters) != 0 {
		t.Errorf("filters length = %d, want 0", len(plan.filters))
	}
}

// TestSelectPlan_AddProjectField tests adding projection fields
func TestSelectPlan_AddProjectField(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		aggOp     string
		wantAgg   bool
		wantLen   int
	}{
		{
			name:      "Simple field without aggregation",
			fieldName: "users.name",
			aggOp:     "",
			wantAgg:   false,
			wantLen:   1,
		},
		{
			name:      "Field with COUNT aggregation",
			fieldName: "*",
			aggOp:     "COUNT",
			wantAgg:   true,
			wantLen:   1,
		},
		{
			name:      "Field with SUM aggregation",
			fieldName: "orders.total",
			aggOp:     "SUM",
			wantAgg:   true,
			wantLen:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := NewSelectPlan()
			err := plan.AddProjectField(tt.fieldName, tt.aggOp)

			if err != nil {
				t.Errorf("AddProjectField() error = %v, want nil", err)
			}
			if len(plan.selectList) != tt.wantLen {
				t.Errorf("selectList length = %d, want %d", len(plan.selectList), tt.wantLen)
			}
			if plan.hasAgg != tt.wantAgg {
				t.Errorf("hasAgg = %v, want %v", plan.hasAgg, tt.wantAgg)
			}
			if tt.wantAgg {
				if plan.aggOp != tt.aggOp {
					t.Errorf("aggOp = %v, want %v", plan.aggOp, tt.aggOp)
				}
				if plan.aggField != tt.fieldName {
					t.Errorf("aggField = %v, want %v", plan.aggField, tt.fieldName)
				}
			}
		})
	}
}

// TestSelectPlan_AddMultipleProjectFields tests adding multiple projection fields
func TestSelectPlan_AddMultipleProjectFields(t *testing.T) {
	plan := NewSelectPlan()

	fields := []struct {
		name  string
		aggOp string
	}{
		{"users.id", ""},
		{"users.name", ""},
		{"users.age", ""},
	}

	for _, f := range fields {
		err := plan.AddProjectField(f.name, f.aggOp)
		if err != nil {
			t.Fatalf("AddProjectField(%s, %s) error = %v", f.name, f.aggOp, err)
		}
	}

	if len(plan.selectList) != len(fields) {
		t.Errorf("selectList length = %d, want %d", len(plan.selectList), len(fields))
	}

	for i, f := range fields {
		if plan.selectList[i].FieldName != f.name {
			t.Errorf("selectList[%d].FieldName = %v, want %v", i, plan.selectList[i].FieldName, f.name)
		}
	}
}

// TestSelectPlan_AddScan tests adding table scans
func TestSelectPlan_AddScan(t *testing.T) {
	plan := NewSelectPlan()

	plan.AddScan("users", "u")
	plan.AddScan("orders", "o")

	if len(plan.tables) != 2 {
		t.Errorf("tables length = %d, want 2", len(plan.tables))
	}
	if plan.tables[0].TableName != "users" {
		t.Errorf("tables[0].TableName = %v, want users", plan.tables[0].TableName)
	}
	if plan.tables[0].Alias != "u" {
		t.Errorf("tables[0].Alias = %v, want u", plan.tables[0].Alias)
	}
	if plan.tables[1].TableName != "orders" {
		t.Errorf("tables[1].TableName = %v, want orders", plan.tables[1].TableName)
	}
	if plan.tables[1].Alias != "o" {
		t.Errorf("tables[1].Alias = %v, want o", plan.tables[1].Alias)
	}
}

// TestSelectPlan_AddFilter tests adding filters
func TestSelectPlan_AddFilter(t *testing.T) {
	tests := []struct {
		name      string
		field     string
		predicate types.Predicate
		constant  string
		wantErr   bool
	}{
		{
			name:      "Valid qualified field",
			field:     "users.id",
			predicate: types.Equals,
			constant:  "1",
			wantErr:   false,
		},
		{
			name:      "Invalid unqualified field",
			field:     "id",
			predicate: types.Equals,
			constant:  "1",
			wantErr:   true,
		},
		{
			name:      "Valid complex field",
			field:     "orders.total",
			predicate: types.GreaterThan,
			constant:  "100.50",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := NewSelectPlan()
			err := plan.AddFilter(tt.field, tt.predicate, tt.constant)

			if (err != nil) != tt.wantErr {
				t.Errorf("AddFilter() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && len(plan.filters) != 1 {
				t.Errorf("filters length = %d, want 1", len(plan.filters))
			}
		})
	}
}

// TestSelectPlan_AddJoin tests adding joins
func TestSelectPlan_AddJoin(t *testing.T) {
	plan := NewSelectPlan()

	rightTable := NewScanNode("orders", "o")
	plan.AddJoin(rightTable, InnerJoin, "users.id", "orders.user_id", types.Equals)

	if len(plan.joins) != 1 {
		t.Errorf("joins length = %d, want 1", len(plan.joins))
	}
	if plan.joins[0].RightTable != rightTable {
		t.Error("join RightTable mismatch")
	}
	if plan.joins[0].JoinType != InnerJoin {
		t.Errorf("join JoinType = %v, want %v", plan.joins[0].JoinType, InnerJoin)
	}
	if plan.joins[0].LeftField != "users.id" {
		t.Errorf("join LeftField = %v, want users.id", plan.joins[0].LeftField)
	}
	if plan.joins[0].RightField != "orders.user_id" {
		t.Errorf("join RightField = %v, want orders.user_id", plan.joins[0].RightField)
	}
}

// TestSelectPlan_SetGroupBy tests setting GROUP BY
func TestSelectPlan_SetGroupBy(t *testing.T) {
	plan := NewSelectPlan()

	plan.SetGroupBy("users.age")

	if plan.groupByField != "users.age" {
		t.Errorf("groupByField = %v, want users.age", plan.groupByField)
	}
}

// TestSelectPlan_AddOrderBy tests adding ORDER BY
func TestSelectPlan_AddOrderBy(t *testing.T) {
	tests := []struct {
		name      string
		field     string
		ascending bool
	}{
		{
			name:      "Order by ascending",
			field:     "users.name",
			ascending: true,
		},
		{
			name:      "Order by descending",
			field:     "users.age",
			ascending: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := NewSelectPlan()
			plan.AddOrderBy(tt.field, tt.ascending)

			if !plan.hasOrderBy {
				t.Error("hasOrderBy = false, want true")
			}
			if plan.orderByField != tt.field {
				t.Errorf("orderByField = %v, want %v", plan.orderByField, tt.field)
			}
			if plan.orderByAsc != tt.ascending {
				t.Errorf("orderByAsc = %v, want %v", plan.orderByAsc, tt.ascending)
			}
		})
	}
}

// TestSelectPlan_SetSelectAll tests setting SELECT *
func TestSelectPlan_SetSelectAll(t *testing.T) {
	plan := NewSelectPlan()

	if plan.SelectAll() {
		t.Error("SelectAll() = true, want false initially")
	}

	plan.SetSelectAll(true)

	if !plan.SelectAll() {
		t.Error("SelectAll() = false, want true after setting")
	}

	plan.SetSelectAll(false)

	if plan.SelectAll() {
		t.Error("SelectAll() = true, want false after unsetting")
	}
}

// TestSelectPlan_Accessors tests all accessor methods
func TestSelectPlan_Accessors(t *testing.T) {
	plan := NewSelectPlan()

	// Add data
	plan.AddProjectField("users.name", "")
	plan.AddScan("users", "u")
	plan.AddFilter("users.id", types.Equals, "1")
	rightTable := NewScanNode("orders", "o")
	plan.AddJoin(rightTable, InnerJoin, "users.id", "orders.user_id", types.Equals)
	plan.SetGroupBy("users.age")
	plan.AddOrderBy("users.name", true)
	plan.SetSelectAll(false)

	// Test accessors
	if len(plan.SelectList()) != 1 {
		t.Errorf("SelectList() length = %d, want 1", len(plan.SelectList()))
	}
	if len(plan.Tables()) != 1 {
		t.Errorf("Tables() length = %d, want 1", len(plan.Tables()))
	}
	if len(plan.Filters()) != 1 {
		t.Errorf("Filters() length = %d, want 1", len(plan.Filters()))
	}
	if len(plan.Joins()) != 1 {
		t.Errorf("Joins() length = %d, want 1", len(plan.Joins()))
	}
	if plan.GroupByField() != "users.age" {
		t.Errorf("GroupByField() = %v, want users.age", plan.GroupByField())
	}
	if !plan.HasOrderBy() {
		t.Error("HasOrderBy() = false, want true")
	}
	if plan.OrderByField() != "users.name" {
		t.Errorf("OrderByField() = %v, want users.name", plan.OrderByField())
	}
	if !plan.OrderByAsc() {
		t.Error("OrderByAsc() = false, want true")
	}
	if plan.SelectAll() {
		t.Error("SelectAll() = true, want false")
	}
}

// TestSelectPlan_AggregationAccessors tests aggregation-related accessors
func TestSelectPlan_AggregationAccessors(t *testing.T) {
	plan := NewSelectPlan()

	// Initially no aggregation
	if plan.HasAgg() {
		t.Error("HasAgg() = true, want false initially")
	}
	if plan.AggOp() != "" {
		t.Errorf("AggOp() = %v, want empty string initially", plan.AggOp())
	}
	if plan.AggField() != "" {
		t.Errorf("AggField() = %v, want empty string initially", plan.AggField())
	}

	// Add aggregation
	plan.AddProjectField("users.age", "AVG")

	if !plan.HasAgg() {
		t.Error("HasAgg() = false, want true after adding aggregation")
	}
	if plan.AggOp() != "AVG" {
		t.Errorf("AggOp() = %v, want AVG", plan.AggOp())
	}
	if plan.AggField() != "users.age" {
		t.Errorf("AggField() = %v, want users.age", plan.AggField())
	}
}

// TestSelectPlan_ComplexQuery tests building a complex query plan
func TestSelectPlan_ComplexQuery(t *testing.T) {
	plan := NewSelectPlan()

	// Build a complex query: SELECT users.name, AVG(orders.total) FROM users u INNER JOIN orders o ON users.id = orders.user_id WHERE users.active = true GROUP BY users.name ORDER BY users.name ASC
	plan.AddProjectField("users.name", "")
	plan.AddProjectField("orders.total", "AVG")
	plan.AddScan("users", "u")
	rightTable := NewScanNode("orders", "o")
	plan.AddJoin(rightTable, InnerJoin, "users.id", "orders.user_id", types.Equals)
	plan.AddFilter("users.active", types.Equals, "true")
	plan.SetGroupBy("users.name")
	plan.AddOrderBy("users.name", true)

	// Verify all components
	if len(plan.SelectList()) != 2 {
		t.Errorf("SelectList() length = %d, want 2", len(plan.SelectList()))
	}
	if len(plan.Tables()) != 1 {
		t.Errorf("Tables() length = %d, want 1", len(plan.Tables()))
	}
	if len(plan.Joins()) != 1 {
		t.Errorf("Joins() length = %d, want 1", len(plan.Joins()))
	}
	if len(plan.Filters()) != 1 {
		t.Errorf("Filters() length = %d, want 1", len(plan.Filters()))
	}
	if !plan.HasAgg() {
		t.Error("HasAgg() = false, want true")
	}
	if plan.AggOp() != "AVG" {
		t.Errorf("AggOp() = %v, want AVG", plan.AggOp())
	}
	if plan.GroupByField() != "users.name" {
		t.Errorf("GroupByField() = %v, want users.name", plan.GroupByField())
	}
	if !plan.HasOrderBy() {
		t.Error("HasOrderBy() = false, want true")
	}
	if plan.OrderByField() != "users.name" {
		t.Errorf("OrderByField() = %v, want users.name", plan.OrderByField())
	}
	if !plan.OrderByAsc() {
		t.Error("OrderByAsc() = false, want true")
	}
}
