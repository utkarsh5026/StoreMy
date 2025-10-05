package plan

import (
	"storemy/pkg/types"
	"testing"
)

// TestScanNode tests the ScanNode creation and String method
func TestScanNode(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		alias     string
		wantStr   string
	}{
		{
			name:      "Basic scan with alias",
			tableName: "users",
			alias:     "u",
			wantStr:   "Scan[table=users, alias=u]",
		},
		{
			name:      "Scan without alias",
			tableName: "orders",
			alias:     "",
			wantStr:   "Scan[table=orders, alias=]",
		},
		{
			name:      "Scan with same table and alias",
			tableName: "products",
			alias:     "products",
			wantStr:   "Scan[table=products, alias=products]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewScanNode(tt.tableName, tt.alias)

			if node.TableName != tt.tableName {
				t.Errorf("TableName = %v, want %v", node.TableName, tt.tableName)
			}
			if node.Alias != tt.alias {
				t.Errorf("Alias = %v, want %v", node.Alias, tt.alias)
			}
			if got := node.String(); got != tt.wantStr {
				t.Errorf("String() = %v, want %v", got, tt.wantStr)
			}
		})
	}
}

// TestJoinType tests the JoinType String method
func TestJoinType(t *testing.T) {
	tests := []struct {
		name     string
		joinType JoinType
		want     string
	}{
		{name: "CrossJoin", joinType: CrossJoin, want: "CROSS"},
		{name: "InnerJoin", joinType: InnerJoin, want: "INNER"},
		{name: "LeftJoin", joinType: LeftJoin, want: "LEFT"},
		{name: "RightJoin", joinType: RightJoin, want: "RIGHT"},
		{name: "Unknown", joinType: JoinType(999), want: "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.joinType.String(); got != tt.want {
				t.Errorf("JoinType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestJoinNode tests the JoinNode creation and String method
func TestJoinNode(t *testing.T) {
	tests := []struct {
		name       string
		rightTable *ScanNode
		joinType   JoinType
		leftField  string
		rightField string
		predicate  types.Predicate
		wantStr    string
	}{
		{
			name:       "Inner join on user_id",
			rightTable: NewScanNode("orders", "o"),
			joinType:   InnerJoin,
			leftField:  "users.id",
			rightField: "orders.user_id",
			predicate:  types.Equals,
			wantStr:    "INNER JOIN orders ON users.id = orders.user_id",
		},
		{
			name:       "Left join",
			rightTable: NewScanNode("products", "p"),
			joinType:   LeftJoin,
			leftField:  "orders.product_id",
			rightField: "products.id",
			predicate:  types.Equals,
			wantStr:    "LEFT JOIN products ON orders.product_id = products.id",
		},
		{
			name:       "Right join",
			rightTable: NewScanNode("categories", "c"),
			joinType:   RightJoin,
			leftField:  "products.category_id",
			rightField: "categories.id",
			predicate:  types.Equals,
			wantStr:    "RIGHT JOIN categories ON products.category_id = categories.id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewJoinNode(tt.rightTable, tt.joinType, tt.leftField, tt.rightField, tt.predicate)

			if node.RightTable != tt.rightTable {
				t.Errorf("RightTable = %v, want %v", node.RightTable, tt.rightTable)
			}
			if node.JoinType != tt.joinType {
				t.Errorf("JoinType = %v, want %v", node.JoinType, tt.joinType)
			}
			if node.LeftField != tt.leftField {
				t.Errorf("LeftField = %v, want %v", node.LeftField, tt.leftField)
			}
			if node.RightField != tt.rightField {
				t.Errorf("RightField = %v, want %v", node.RightField, tt.rightField)
			}
			if node.Predicate != tt.predicate {
				t.Errorf("Predicate = %v, want %v", node.Predicate, tt.predicate)
			}
			if got := node.String(); got != tt.wantStr {
				t.Errorf("String() = %v, want %v", got, tt.wantStr)
			}
		})
	}
}

// TestFilterNode tests the FilterNode creation and String method
func TestFilterNode(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		field     string
		predicate types.Predicate
		constant  string
		wantStr   string
	}{
		{
			name:      "Equals filter",
			table:     "u",
			field:     "u.id",
			predicate: types.Equals,
			constant:  "1",
			wantStr:   "Filter[u.u.id = 1]",
		},
		{
			name:      "Greater than filter",
			table:     "users",
			field:     "users.age",
			predicate: types.GreaterThan,
			constant:  "25",
			wantStr:   "Filter[users.users.age > 25]",
		},
		{
			name:      "String comparison",
			table:     "u",
			field:     "u.name",
			predicate: types.Equals,
			constant:  "John",
			wantStr:   "Filter[u.u.name = John]",
		},
		{
			name:      "Less than or equal",
			table:     "products",
			field:     "products.price",
			predicate: types.LessThanOrEqual,
			constant:  "100.50",
			wantStr:   "Filter[products.products.price <= 100.50]",
		},
		{
			name:      "Not equal filter",
			table:     "users",
			field:     "users.status",
			predicate: types.NotEqual,
			constant:  "inactive",
			wantStr:   "Filter[users.users.status != inactive]",
		},
		{
			name:      "Greater than or equal",
			table:     "orders",
			field:     "orders.amount",
			predicate: types.GreaterThanOrEqual,
			constant:  "1000",
			wantStr:   "Filter[orders.orders.amount >= 1000]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewFilterNode(tt.table, tt.field, tt.predicate, tt.constant)

			if node.Table != tt.table {
				t.Errorf("Table = %v, want %v", node.Table, tt.table)
			}
			if node.Field != tt.field {
				t.Errorf("Field = %v, want %v", node.Field, tt.field)
			}
			if node.Predicate != tt.predicate {
				t.Errorf("Predicate = %v, want %v", node.Predicate, tt.predicate)
			}
			if node.Constant != tt.constant {
				t.Errorf("Constant = %v, want %v", node.Constant, tt.constant)
			}
			if got := node.String(); got != tt.wantStr {
				t.Errorf("String() = %v, want %v", got, tt.wantStr)
			}
		})
	}
}

// TestSelectListNode tests the SelectListNode creation and String method
func TestSelectListNode(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		aggOp     string
		wantStr   string
	}{
		{
			name:      "Simple field",
			fieldName: "users.name",
			aggOp:     "",
			wantStr:   "users.name",
		},
		{
			name:      "COUNT aggregation",
			fieldName: "*",
			aggOp:     "COUNT",
			wantStr:   "COUNT(*)",
		},
		{
			name:      "SUM aggregation",
			fieldName: "orders.total",
			aggOp:     "SUM",
			wantStr:   "SUM(orders.total)",
		},
		{
			name:      "AVG aggregation",
			fieldName: "products.price",
			aggOp:     "AVG",
			wantStr:   "AVG(products.price)",
		},
		{
			name:      "MAX aggregation",
			fieldName: "users.age",
			aggOp:     "MAX",
			wantStr:   "MAX(users.age)",
		},
		{
			name:      "MIN aggregation",
			fieldName: "users.age",
			aggOp:     "MIN",
			wantStr:   "MIN(users.age)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewSelectListNode(tt.fieldName, tt.aggOp)

			if node.FieldName != tt.fieldName {
				t.Errorf("FieldName = %v, want %v", node.FieldName, tt.fieldName)
			}
			if node.AggOp != tt.aggOp {
				t.Errorf("AggOp = %v, want %v", node.AggOp, tt.aggOp)
			}
			if got := node.String(); got != tt.wantStr {
				t.Errorf("String() = %v, want %v", got, tt.wantStr)
			}
		})
	}
}
