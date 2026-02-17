package parser

import (
	"storemy/pkg/parser/statements"
	"storemy/pkg/plan"
	"testing"
)

// SELECT statement tests

func TestParseStatement_BasicSelect(t *testing.T) {
	stmt, err := ParseStatement("SELECT name FROM users")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if len(selectStmt.Plan.SelectList()) != 1 {
		t.Errorf("expected 1 select field, got %d", len(selectStmt.Plan.SelectList()))
	}

	if len(selectStmt.Plan.Tables()) != 1 {
		t.Errorf("expected 1 table, got %d", len(selectStmt.Plan.Tables()))
	}

	if selectStmt.Plan.Tables()[0].TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", selectStmt.Plan.Tables()[0].TableName)
	}
}

func TestParseStatement_SelectMultipleFields(t *testing.T) {
	stmt, err := ParseStatement("SELECT name, age, email FROM users")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if len(selectStmt.Plan.SelectList()) != 3 {
		t.Errorf("expected 3 select fields, got %d", len(selectStmt.Plan.SelectList()))
	}

	expectedFields := []string{"NAME", "AGE", "EMAIL"}
	for i, expected := range expectedFields {
		if selectStmt.Plan.SelectList()[i].FieldName != expected {
			t.Errorf("expected field name '%s', got %s", expected, selectStmt.Plan.SelectList()[i].FieldName)
		}
	}
}

func TestParseStatement_SelectWithTableAlias(t *testing.T) {
	stmt, err := ParseStatement("SELECT name FROM users u")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if len(selectStmt.Plan.Tables()) != 1 {
		t.Errorf("expected 1 table, got %d", len(selectStmt.Plan.Tables()))
	}

	if selectStmt.Plan.Tables()[0].TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", selectStmt.Plan.Tables()[0].TableName)
	}

	if selectStmt.Plan.Tables()[0].Alias != "U" {
		t.Errorf("expected alias 'U', got %s", selectStmt.Plan.Tables()[0].Alias)
	}
}

func TestParseStatement_SelectWithWhereClause(t *testing.T) {
	stmt, err := ParseStatement("SELECT name FROM users u WHERE u.id = 1")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if len(selectStmt.Plan.Filters()) != 1 {
		t.Errorf("expected 1 filter, got %d", len(selectStmt.Plan.Filters()))
	}

	filter := selectStmt.Plan.Filters()[0]
	if filter.Field != "U.ID" {
		t.Errorf("expected field name 'U.ID', got %s", filter.Field)
	}

	if filter.Constant != "1" {
		t.Errorf("expected constant '1', got %s", filter.Constant)
	}
}

func TestParseStatement_SelectWithMultipleWhereConditions(t *testing.T) {
	stmt, err := ParseStatement("SELECT name FROM users u WHERE u.id = 1 AND u.active = 'true'")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if len(selectStmt.Plan.Filters()) != 2 {
		t.Errorf("expected 2 filters, got %d", len(selectStmt.Plan.Filters()))
	}

	// Check first filter
	filter1 := selectStmt.Plan.Filters()[0]
	if filter1.Field != "U.ID" {
		t.Errorf("expected first field name 'U.ID', got %s", filter1.Field)
	}

	// Check second filter
	filter2 := selectStmt.Plan.Filters()[1]
	if filter2.Field != "U.ACTIVE" {
		t.Errorf("expected second field name 'U.ACTIVE', got %s", filter2.Field)
	}
}

func TestParseStatement_SelectWithAggregateFunction(t *testing.T) {
	stmt, err := ParseStatement("SELECT COUNT(id) FROM users")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if len(selectStmt.Plan.SelectList()) != 1 {
		t.Errorf("expected 1 select field, got %d", len(selectStmt.Plan.SelectList()))
	}

	selectField := selectStmt.Plan.SelectList()[0]
	if selectField.FieldName != "ID" {
		t.Errorf("expected field name 'ID', got %s", selectField.FieldName)
	}

	if selectField.AggOp != "COUNT" {
		t.Errorf("expected aggregate operation 'COUNT', got %s", selectField.AggOp)
	}

	if !selectStmt.Plan.HasAgg() {
		t.Error("expected plan to have aggregation")
	}
}

func TestParseStatement_SelectWithGroupBy(t *testing.T) {
	stmt, err := ParseStatement("SELECT name FROM users GROUP BY name")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if selectStmt.Plan.GroupByField() != "NAME" {
		t.Errorf("expected group by field 'NAME', got %s", selectStmt.Plan.GroupByField())
	}
}

func TestParseStatement_SelectWithOrderByAsc(t *testing.T) {
	stmt, err := ParseStatement("SELECT name FROM users ORDER BY name")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if !selectStmt.Plan.HasOrderBy() {
		t.Error("expected plan to have ORDER BY")
	}

	if selectStmt.Plan.OrderByField() != "NAME" {
		t.Errorf("expected order by field 'NAME', got %s", selectStmt.Plan.OrderByField())
	}

	if !selectStmt.Plan.OrderByAsc() {
		t.Error("expected ascending order by default")
	}
}

func TestParseStatement_SelectWithOrderByDesc(t *testing.T) {
	stmt, err := ParseStatement("SELECT name FROM users ORDER BY name DESC")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if !selectStmt.Plan.HasOrderBy() {
		t.Error("expected plan to have ORDER BY")
	}

	if selectStmt.Plan.OrderByAsc() {
		t.Error("expected descending order")
	}
}

func TestParseStatement_ComplexSelect(t *testing.T) {
	stmt, err := ParseStatement("SELECT name, COUNT(id) FROM users u WHERE u.active = 'true' GROUP BY name ORDER BY name DESC")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	// Check select fields
	if len(selectStmt.Plan.SelectList()) != 2 {
		t.Errorf("expected 2 select fields, got %d", len(selectStmt.Plan.SelectList()))
	}

	// Check aggregation
	if !selectStmt.Plan.HasAgg() {
		t.Error("expected plan to have aggregation")
	}

	// Check WHERE clause
	if len(selectStmt.Plan.Filters()) != 1 {
		t.Errorf("expected 1 filter, got %d", len(selectStmt.Plan.Filters()))
	}

	// Check GROUP BY
	if selectStmt.Plan.GroupByField() != "NAME" {
		t.Errorf("expected group by field 'NAME', got %s", selectStmt.Plan.GroupByField())
	}

	// Check ORDER BY
	if !selectStmt.Plan.HasOrderBy() {
		t.Error("expected plan to have ORDER BY")
	}

	if selectStmt.Plan.OrderByAsc() {
		t.Error("expected descending order")
	}
}

// SELECT statement error handling tests

func TestParseSelectStatement_MissingFrom(t *testing.T) {
	lexer := NewLexer("name")

	_, err := (&SelectParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing FROM")
	}

	if err.Error() != "expected SELECT, got NAME" {
		t.Errorf("expected 'expected SELECT, got NAME', got %s", err.Error())
	}
}

func TestParseSelectStatement_MissingTableName(t *testing.T) {
	lexer := NewLexer("FROM")

	_, err := (&SelectParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing table name")
	}

	if err.Error() != "expected SELECT, got FROM" {
		t.Errorf("expected 'expected SELECT, got FROM', got %s", err.Error())
	}
}

func TestParseSelectStatement_InvalidFieldName(t *testing.T) {
	lexer := NewLexer("123 FROM users")

	_, err := (&SelectParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for invalid field name")
	}

	if err.Error() != "expected SELECT, got 123" {
		t.Errorf("expected 'expected SELECT, got 123', got %s", err.Error())
	}
}

// Test helper functions

func TestParseSelect_SingleField(t *testing.T) {
	lexer := NewLexer("SELECT name FROM")
	selectPlan := plan.NewSelectPlan()

	err := (&SelectParser{}).parseSelect(lexer, selectPlan)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(selectPlan.SelectList()) != 1 {
		t.Errorf("expected 1 select field, got %d", len(selectPlan.SelectList()))
	}

	if selectPlan.SelectList()[0].FieldName != "NAME" {
		t.Errorf("expected field name 'NAME', got %s", selectPlan.SelectList()[0].FieldName)
	}
}

func TestParseSelect_MultipleFields(t *testing.T) {
	lexer := NewLexer("SELECT name, age, email FROM")
	selectPlan := plan.NewSelectPlan()

	err := (&SelectParser{}).parseSelect(lexer, selectPlan)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(selectPlan.SelectList()) != 3 {
		t.Errorf("expected 3 select fields, got %d", len(selectPlan.SelectList()))
	}

	expectedFields := []string{"NAME", "AGE", "EMAIL"}
	for i, expected := range expectedFields {
		if selectPlan.SelectList()[i].FieldName != expected {
			t.Errorf("expected field name '%s', got %s", expected, selectPlan.SelectList()[i].FieldName)
		}
	}
}

func TestParseSelectField_RegularField(t *testing.T) {
	l := NewLexer("FROM")
	selectPlan := plan.NewSelectPlan()
	fieldToken := Token{Type: IDENTIFIER, Value: "name"}

	err := (&SelectParser{}).parseSelectField(l, selectPlan, fieldToken)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(selectPlan.SelectList()) != 1 {
		t.Errorf("expected 1 select field, got %d", len(selectPlan.SelectList()))
	}

	if selectPlan.SelectList()[0].FieldName != "NAME" {
		t.Errorf("expected field name 'NAME', got %s", selectPlan.SelectList()[0].FieldName)
	}

	if selectPlan.SelectList()[0].AggOp != "" {
		t.Errorf("expected no aggregate operation, got %s", selectPlan.SelectList()[0].AggOp)
	}
}

func TestParseSelectField_AggregateFunction(t *testing.T) {
	l := NewLexer("(id)")
	selectPlan := plan.NewSelectPlan()
	fieldToken := Token{Type: IDENTIFIER, Value: "COUNT"}

	err := (&SelectParser{}).parseSelectField(l, selectPlan, fieldToken)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(selectPlan.SelectList()) != 1 {
		t.Errorf("expected 1 select field, got %d", len(selectPlan.SelectList()))
	}

	if selectPlan.SelectList()[0].FieldName != "ID" {
		t.Errorf("expected field name 'ID', got %s", selectPlan.SelectList()[0].FieldName)
	}

	if selectPlan.SelectList()[0].AggOp != "COUNT" {
		t.Errorf("expected aggregate operation 'COUNT', got %s", selectPlan.SelectList()[0].AggOp)
	}
}

func TestParseFrom_SingleTable(t *testing.T) {
	lexer := NewLexer("FROM users")
	selectPlan := plan.NewSelectPlan()

	err := (&SelectParser{}).parseFrom(lexer, selectPlan)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(selectPlan.Tables()) != 1 {
		t.Errorf("expected 1 table, got %d", len(selectPlan.Tables()))
	}

	if selectPlan.Tables()[0].TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", selectPlan.Tables()[0].TableName)
	}
}

func TestParseTable_WithAlias(t *testing.T) {
	lexer := NewLexer("users u")
	selectPlan := plan.NewSelectPlan()

	err := (&SelectParser{}).parseTable(lexer, selectPlan)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(selectPlan.Tables()) != 1 {
		t.Errorf("expected 1 table, got %d", len(selectPlan.Tables()))
	}

	if selectPlan.Tables()[0].TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", selectPlan.Tables()[0].TableName)
	}

	if selectPlan.Tables()[0].Alias != "U" {
		t.Errorf("expected alias 'U', got %s", selectPlan.Tables()[0].Alias)
	}
}

func TestParseWhere_NoWhereClause(t *testing.T) {
	lexer := NewLexer("ORDER BY")
	selectPlan := plan.NewSelectPlan()

	err := (&SelectParser{}).parseWhere(lexer, selectPlan)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(selectPlan.Filters()) != 0 {
		t.Errorf("expected no filters, got %d", len(selectPlan.Filters()))
	}
}

func TestParseWhere_WithCondition(t *testing.T) {
	lexer := NewLexer("u.id = 1")
	selectPlan := plan.NewSelectPlan()

	err := parseConditions(lexer, selectPlan)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(selectPlan.Filters()) != 1 {
		t.Errorf("expected 1 filter, got %d", len(selectPlan.Filters()))
	}

	filter := selectPlan.Filters()[0]
	if filter.Field != "U.ID" {
		t.Errorf("expected field name 'U.ID', got %s", filter.Field)
	}

	if filter.Constant != "1" {
		t.Errorf("expected constant '1', got %s", filter.Constant)
	}
}

func TestParseGroupBy_NoGroupBy(t *testing.T) {
	lexer := NewLexer("ORDER BY")
	selectPlan := plan.NewSelectPlan()

	err := (&SelectParser{}).parseGroupBy(lexer, selectPlan)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if selectPlan.GroupByField() != "" {
		t.Errorf("expected no group by field, got %s", selectPlan.GroupByField())
	}
}

func TestParseGroupBy_WithGroupBy(t *testing.T) {
	lexer := NewLexer("GROUP BY name")
	selectPlan := plan.NewSelectPlan()

	err := (&SelectParser{}).parseGroupBy(lexer, selectPlan)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if selectPlan.GroupByField() != "NAME" {
		t.Errorf("expected group by field 'NAME', got %s", selectPlan.GroupByField())
	}
}

func TestParseOrderBy_NoOrderBy(t *testing.T) {
	lexer := NewLexer("EOF")
	selectPlan := plan.NewSelectPlan()

	err := (&SelectParser{}).parseOrderBy(lexer, selectPlan)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if selectPlan.HasOrderBy() {
		t.Error("expected no order by")
	}
}

func TestParseOrderBy_WithOrderBy(t *testing.T) {
	lexer := NewLexer("ORDER BY name DESC")
	selectPlan := plan.NewSelectPlan()

	err := (&SelectParser{}).parseOrderBy(lexer, selectPlan)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if !selectPlan.HasOrderBy() {
		t.Error("expected order by")
	}

	if selectPlan.OrderByField() != "NAME" {
		t.Errorf("expected order by field 'NAME', got %s", selectPlan.OrderByField())
	}

	if selectPlan.OrderByAsc() {
		t.Error("expected descending order")
	}
}

func TestConsumeCommaIfPresent_WithComma(t *testing.T) {
	lexer := NewLexer(",")

	result := consumeCommaIfPresent(lexer)
	if !result {
		t.Error("expected comma to be consumed")
	}
}

func TestConsumeCommaIfPresent_WithoutComma(t *testing.T) {
	lexer := NewLexer("FROM")

	result := consumeCommaIfPresent(lexer)
	if result {
		t.Error("expected comma not to be consumed")
	}
}

// Test for unqualified column names in WHERE clauses
func TestParseStatement_SelectWithUnqualifiedColumnInWhere(t *testing.T) {
	stmt, err := ParseStatement("SELECT name FROM users WHERE age > 30")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if len(selectStmt.Plan.Filters()) != 1 {
		t.Errorf("expected 1 filter, got %d", len(selectStmt.Plan.Filters()))
	}

	filter := selectStmt.Plan.Filters()[0]
	// The field should be auto-qualified with the table name
	if filter.Field != "USERS.AGE" {
		t.Errorf("expected field name 'USERS.AGE', got %s", filter.Field)
	}

	if filter.Constant != "30" {
		t.Errorf("expected constant '30', got %s", filter.Constant)
	}
}

func TestParseStatement_SelectWithUnqualifiedColumnAndAlias(t *testing.T) {
	stmt, err := ParseStatement("SELECT name FROM users u WHERE id = 1")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if len(selectStmt.Plan.Filters()) != 1 {
		t.Errorf("expected 1 filter, got %d", len(selectStmt.Plan.Filters()))
	}

	filter := selectStmt.Plan.Filters()[0]
	// When alias exists, it should use the alias for qualification
	if filter.Field != "U.ID" {
		t.Errorf("expected field name 'U.ID', got %s", filter.Field)
	}

	if filter.Constant != "1" {
		t.Errorf("expected constant '1', got %s", filter.Constant)
	}
}

func TestParseStatement_SelectWithMultipleUnqualifiedConditions(t *testing.T) {
	stmt, err := ParseStatement("SELECT name FROM users WHERE age > 30 AND id < 100")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatal("expected SelectStatement")
	}

	if len(selectStmt.Plan.Filters()) != 2 {
		t.Errorf("expected 2 filters, got %d", len(selectStmt.Plan.Filters()))
	}

	// Check first filter
	filter1 := selectStmt.Plan.Filters()[0]
	if filter1.Field != "USERS.AGE" {
		t.Errorf("expected first field name 'USERS.AGE', got %s", filter1.Field)
	}

	// Check second filter
	filter2 := selectStmt.Plan.Filters()[1]
	if filter2.Field != "USERS.ID" {
		t.Errorf("expected second field name 'USERS.ID', got %s", filter2.Field)
	}
}
