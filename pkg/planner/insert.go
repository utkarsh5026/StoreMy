package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/parser/statements"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type DMLResult struct {
	RowsAffected int
	Message      string
}

func (d *DMLResult) String() string {
	return fmt.Sprintf("%d row(s) affected: %s", d.RowsAffected, d.Message)
}

type InsertPlan struct {
	statement    *statements.InsertStatement
	pageStore    *memory.PageStore
	tableManager *memory.TableManager
	tid          *transaction.TransactionID
}

func NewInsertPlan(
	stmt *statements.InsertStatement,
	ps *memory.PageStore,
	tid *transaction.TransactionID,
	tm *memory.TableManager) *InsertPlan {
	return &InsertPlan{
		statement:    stmt,
		pageStore:    ps,
		tableManager: tm,
		tid:          tid,
	}
}

func (p *InsertPlan) Execute() (any, error) {
	tableId, err := p.getTableID()
	if err != nil {
		return nil, err
	}

	tupleDesc, err := p.getTupleDesc()
	if err != nil {
		return nil, err
	}

	fieldMapping, err := p.createFieldMapping(tupleDesc)
	if err != nil {
		return nil, err
	}

	insertedCount, err := p.insertTuples(tableId, tupleDesc, fieldMapping)
	if err != nil {
		return nil, err
	}

	return &DMLResult{
		RowsAffected: insertedCount,
		Message:      fmt.Sprintf("%d row(s) inserted", insertedCount),
	}, nil
}

func (p *InsertPlan) getTableID() (int, error) {
	tableName := p.statement.TableName
	tableID, err := p.tableManager.GetTableID(tableName)
	if err != nil {
		return 0, fmt.Errorf("table %s not found", tableName)
	}
	return tableID, nil
}

func (p *InsertPlan) getTupleDesc() (*tuple.TupleDescription, error) {
	tableName := p.statement.TableName
	tableID, err := p.tableManager.GetTableID(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	tupleDesc, err := p.tableManager.GetTupleDesc(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %s", tableName)
	}

	return tupleDesc, nil
}

func (p *InsertPlan) createFieldMapping(tupleDesc *tuple.TupleDescription) ([]int, error) {
	if len(p.statement.Fields) == 0 {
		return nil, nil
	}

	fieldMapping := make([]int, len(p.statement.Fields))
	for i, field := range p.statement.Fields {
		fieldIndex, err := p.findFieldIndex(field, tupleDesc)
		if err != nil {
			return nil, err
		}
		fieldMapping[i] = fieldIndex
	}
	return fieldMapping, nil
}

func (p *InsertPlan) insertTuples(tableID int, tupleDesc *tuple.TupleDescription, fieldMapping []int) (int, error) {
	insertedCount := 0
	for _, values := range p.statement.Values {
		if err := p.validateValueCount(values, tupleDesc, fieldMapping); err != nil {
			return 0, err
		}

		newTuple, err := p.createTuple(values, tupleDesc, fieldMapping)
		if err != nil {
			return 0, err
		}

		if err := p.pageStore.InsertTuple(p.tid, tableID, newTuple); err != nil {
			return 0, fmt.Errorf("failed to insert tuple: %v", err)
		}

		insertedCount++
	}

	return insertedCount, nil
}

func (p *InsertPlan) validateValueCount(values []types.Field, tupleDesc *tuple.TupleDescription, fieldMapping []int) error {
	var expectedCount int
	if fieldMapping != nil {
		expectedCount = len(fieldMapping)
	} else {
		expectedCount = tupleDesc.NumFields()
	}

	if len(values) != expectedCount {
		return fmt.Errorf("value count mismatch: expected %d, got %d", expectedCount, len(values))
	}

	return nil
}

func (p *InsertPlan) createTuple(values []types.Field, tupleDesc *tuple.TupleDescription, fieldMapping []int) (*tuple.Tuple, error) {
	newTuple := tuple.NewTuple(tupleDesc)
	if fieldMapping != nil {
		for i, value := range values {
			if err := newTuple.SetField(fieldMapping[i], value); err != nil {
				return nil, fmt.Errorf("failed to set field: %v", err)
			}
		}

		for i := 0; i < tupleDesc.NumFields(); i++ {
			if !isFieldMapped(i, fieldMapping) {
				return nil, fmt.Errorf("missing value for field index %d", i)
			}
		}
		return newTuple, nil
	}

	for i, value := range values {
		if err := newTuple.SetField(i, value); err != nil {
			return nil, fmt.Errorf("failed to set field: %v", err)
		}
	}
	return newTuple, nil
}

func (p *InsertPlan) findFieldIndex(fieldName string, tupleDesc *tuple.TupleDescription) (int, error) {
	for i := 0; i < tupleDesc.NumFields(); i++ {
		name, _ := tupleDesc.GetFieldName(i)
		if name == fieldName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in table %s", fieldName, p.statement.TableName)
}

func isFieldMapped(fieldIndex int, fieldMapping []int) bool {
	for _, mappedIndex := range fieldMapping {
		if mappedIndex == fieldIndex {
			return true
		}
	}
	return false
}
