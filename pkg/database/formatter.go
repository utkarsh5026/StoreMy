package database

import (
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner"
)

func formatResult(rawResult any, stmt statements.Statement) (QueryResult, error) {
	switch stmt.GetType() {
	case statements.Select:
		if queryResult, ok := rawResult.(*planner.SelectQueryResult); ok {
			return formatSelect(queryResult), nil
		}

	case statements.Insert, statements.Update, statements.Delete:
		if dmlResult, ok := rawResult.(*planner.DMLResult); ok {
			return formatDML(dmlResult, stmt.GetType()), nil
		}

	case statements.CreateTable, statements.DropTable:
		if ddlResult, ok := rawResult.(*planner.DDLResult); ok {
			return formatDDL(ddlResult), nil
		}
	}

	return QueryResult{
		Success: true,
		Message: "Query executed successfully",
	}, nil
}

// formatSelect converts SELECT query results to standard format
func formatSelect(result *planner.SelectQueryResult) QueryResult {
	if result == nil || result.TupleDesc == nil {
		return QueryResult{
			Success: true,
			Message: "Query returned no results",
			Rows:    [][]string{},
		}
	}

	numFields := result.TupleDesc.NumFields()
	columns := make([]string, numFields)
	for i := range numFields {
		name, _ := result.TupleDesc.GetFieldName(i)
		if name == "" {
			name = fmt.Sprintf("col_%d", i)
		}
		columns[i] = name
	}

	rows := make([][]string, 0, len(result.Tuples))
	for _, tuple := range result.Tuples {
		row := make([]string, numFields)
		for i := range numFields {
			field, err := tuple.GetField(i)
			if err != nil || field == nil {
				row[i] = "NULL"
			} else {
				row[i] = field.String()
			}
		}
		rows = append(rows, row)
	}

	return QueryResult{
		Success: true,
		Columns: columns,
		Rows:    rows,
		Message: fmt.Sprintf("%d row(s) returned", len(rows)),
	}
}

// FormatDML converts DML (INSERT/UPDATE/DELETE) results to standard format
func formatDML(result *planner.DMLResult, stmtType statements.StatementType) QueryResult {
	action := ""
	switch stmtType {
	case statements.Insert:
		action = "inserted"
	case statements.Update:
		action = "updated"
	case statements.Delete:
		action = "deleted"
	}

	return QueryResult{
		Success:      true,
		RowsAffected: result.RowsAffected,
		Message:      fmt.Sprintf("%d row(s) %s", result.RowsAffected, action),
	}
}

// FormatDDL converts DDL (CREATE/DROP TABLE) results to standard format
func formatDDL(result *planner.DDLResult) QueryResult {
	return QueryResult{
		Success: result.Success,
		Message: result.Message,
	}
}
