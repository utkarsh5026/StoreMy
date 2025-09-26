package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/parser/statements"
	"storemy/pkg/tuple"
)

type DMLResult struct {
	RowsAffected int
	Message      string
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
		found := false
		for j := 0; j < tupleDesc.NumFields(); j++ {
			name, _ := tupleDesc.GetFieldName(j)
			if name == field {
				fieldMapping[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column %s not found in table %s", field, p.statement.TableName)
		}
	}
	return fieldMapping, nil
}

func (p *InsertPlan) insertTuples(tableID int, tupleDesc *tuple.TupleDescription, fieldMapping []int) (int, error) {
	insertedCount := 0
	for _, values := range p.statement.Values {
		if fieldMapping != nil {
			if len(values) != len(fieldMapping) {
				return insertedCount, fmt.Errorf("value count mismatch: expected %d, got %d",
					len(fieldMapping), len(values))
			}
		} else {
			if len(values) != tupleDesc.NumFields() {
				return insertedCount, fmt.Errorf("value count mismatch: expected %d, got %d",
					tupleDesc.NumFields(), len(values))
			}
		}

		newTuple := tuple.NewTuple(tupleDesc)
		if fieldMapping != nil {
			for i, value := range values {
				if err := newTuple.SetField(fieldMapping[i], value); err != nil {
					return insertedCount, fmt.Errorf("failed to set field: %v", err)
				}
			}

			// Fill in any missing fields with defaults
			// This is important for partial inserts
			for i := 0; i < tupleDesc.NumFields(); i++ {
				isSet := false
				for _, mappedIndex := range fieldMapping {
					if mappedIndex == i {
						isSet = true
						break
					}
				}
				if !isSet {
					// Set to NULL or default value
					// For now, we'll use NULL (nil)
					newTuple.SetField(i, nil)
				}
			}
		} else {
			for i, value := range values {
				if err := newTuple.SetField(i, value); err != nil {
					return insertedCount, fmt.Errorf("failed to set field: %v", err)
				}
			}
		}

		if err := p.pageStore.InsertTuple(p.tid, tableID, newTuple); err != nil {
			return insertedCount, fmt.Errorf("failed to insert tuple: %v", err)
		}

		insertedCount++
	}

	return insertedCount, nil
}
