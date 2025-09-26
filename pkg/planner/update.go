package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/parser/statements"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type UpdatePlan struct {
	statement    *statements.UpdateStatement
	tableManager *memory.TableManager
	pageStore    *memory.PageStore
	tid          *transaction.TransactionID
}

func NewUpdatePlan(statement *statements.UpdateStatement, tableManager *memory.TableManager, pageStore *memory.PageStore, tid *transaction.TransactionID) *UpdatePlan {
	return &UpdatePlan{
		statement:    statement,
		tableManager: tableManager,
		pageStore:    pageStore,
		tid:          tid,
	}
}

func (p *UpdatePlan) Execute() (any, error) {
	tableID, tupleDesc, err := p.getTableMetadata()
	if err != nil {
		return nil, err
	}

	updateMap, err := p.buildUpdateMap(tupleDesc)
	if err != nil {
		return nil, err
	}

	queryPlan, err := p.buildQueryPlan(tableID)
	if err != nil {
		return nil, err
	}

	tuplesToUpdate, err := p.collectTuplesToUpdate(queryPlan)
	if err != nil {
		return nil, err
	}

	err = p.updateTuples(tuplesToUpdate, tupleDesc, updateMap)
	if err != nil {
		return nil, err
	}

	return &DMLResult{
		RowsAffected: len(tuplesToUpdate),
		Message:      fmt.Sprintf("%d row(s) updated", len(tuplesToUpdate)),
	}, nil
}

func (p *UpdatePlan) getTableMetadata() (int, *tuple.TupleDescription, error) {
	tableID, err := p.tableManager.GetTableID(p.statement.TableName)
	if err != nil {
		return 0, nil, fmt.Errorf("table %s not found", p.statement.TableName)
	}

	tupleDesc, err := p.tableManager.GetTupleDesc(tableID)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get table schema: %v", err)
	}
	return tableID, tupleDesc, nil
}

func (p *UpdatePlan) buildUpdateMap(tupleDesc *tuple.TupleDescription) (map[int]types.Field, error) {
	updateMap := make(map[int]types.Field)

	for _, setClause := range p.statement.SetClauses {
		fieldIndex, err := p.findFieldIndex(setClause.FieldName, tupleDesc)
		if err != nil {
			return nil, err
		}
		updateMap[fieldIndex] = setClause.Value
	}

	return updateMap, nil
}

func (p *UpdatePlan) findFieldIndex(fieldName string, tupleDesc *tuple.TupleDescription) (int, error) {
	fieldCount := tupleDesc.NumFields()
	for i := 0; i < fieldCount; i++ {
		name, _ := tupleDesc.GetFieldName(i)
		if name == fieldName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found", fieldName)
}

// buildQueryPlan creates the query execution plan for finding rows to update
func (p *UpdatePlan) buildQueryPlan(tableID int) (iterator.DbIterator, error) {
	scanOp, err := query.NewSeqScan(p.tid, tableID, p.tableManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %v", err)
	}

	var currentOp iterator.DbIterator = scanOp
	where := p.statement.WhereClause
	if where != nil {
		predicate, err := buildPredicateFromFilterNode(where, scanOp.GetTupleDesc())
		if err != nil {
			return nil, fmt.Errorf("failed to build WHERE predicate: %v", err)
		}

		filterOp, err := query.NewFilter(predicate, currentOp)
		if err != nil {
			return nil, fmt.Errorf("failed to create filter: %v", err)
		}
		currentOp = filterOp
	}

	return currentOp, nil
}

// collectTuplesToUpdate finds all tuples that match the update criteria
// This is done in a separate phase to avoid iterator invalidation issues
func (p *UpdatePlan) collectTuplesToUpdate(queryPlan iterator.DbIterator) ([]*tuple.Tuple, error) {
	if err := queryPlan.Open(); err != nil {
		return nil, fmt.Errorf("failed to open update query: %v", err)
	}
	defer queryPlan.Close()

	var tuplesToUpdate []*tuple.Tuple

	for {
		hasNext, err := queryPlan.HasNext()
		if err != nil {
			return nil, fmt.Errorf("error during update scan: %v", err)
		}

		if !hasNext {
			break
		}

		tuple, err := queryPlan.Next()
		if err != nil {
			return nil, fmt.Errorf("error fetching tuple to update: %v", err)
		}

		tuplesToUpdate = append(tuplesToUpdate, tuple)
	}

	return tuplesToUpdate, nil
}

// updateTuples applies updates to all collected tuples
// UPDATE is implemented as DELETE + INSERT at the storage layer
func (p *UpdatePlan) updateTuples(tuplesToUpdate []*tuple.Tuple, tupleDesc *tuple.TupleDescription, updateMap map[int]types.Field) error {
	for _, oldTuple := range tuplesToUpdate {
		newTuple := tuple.NewTuple(tupleDesc)

		for i := 0; i < tupleDesc.NumFields(); i++ {
			field, _ := oldTuple.GetField(i)
			newTuple.SetField(i, field)
		}

		for fieldIndex, newValue := range updateMap {
			if err := newTuple.SetField(fieldIndex, newValue); err != nil {
				return fmt.Errorf("failed to set updated field: %v", err)
			}
		}

		if err := p.pageStore.UpdateTuple(p.tid, oldTuple, newTuple); err != nil {
			return fmt.Errorf("failed to update tuple: %v", err)
		}
	}

	return nil
}
