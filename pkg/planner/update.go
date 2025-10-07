package planner

import (
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type UpdatePlan struct {
	statement *statements.UpdateStatement
	ctx       *registry.DatabaseContext
	tid       *primitives.TransactionID
}

func NewUpdatePlan(statement *statements.UpdateStatement, tid *primitives.TransactionID, ctx *registry.DatabaseContext) *UpdatePlan {
	return &UpdatePlan{
		statement: statement,
		ctx:       ctx,
		tid:       tid,
	}
}

func (p *UpdatePlan) Execute() (any, error) {
	md, err := resolveTableMetadata(p.statement.TableName, p.ctx)
	if err != nil {
		return nil, err
	}

	updateMap, err := p.buildUpdateMap(md.TupleDesc)
	if err != nil {
		return nil, err
	}

	queryPlan, err := buildScanWithFilter(p.tid, md.TableID, p.statement.WhereClause, p.ctx)
	if err != nil {
		return nil, err
	}

	tuplesToUpdate, err := collectAllTuples(queryPlan)
	if err != nil {
		return nil, err
	}

	err = p.updateTuples(tuplesToUpdate, md.TupleDesc, updateMap)
	if err != nil {
		return nil, err
	}

	return &DMLResult{
		RowsAffected: len(tuplesToUpdate),
		Message:      fmt.Sprintf("%d row(s) updated", len(tuplesToUpdate)),
	}, nil
}

func (p *UpdatePlan) buildUpdateMap(tupleDesc *tuple.TupleDescription) (map[int]types.Field, error) {
	updateMap := make(map[int]types.Field)

	for _, cl := range p.statement.SetClauses {
		i, err := findFieldIndex(cl.FieldName, tupleDesc)
		if err != nil {
			return nil, err
		}
		updateMap[i] = cl.Value
	}

	return updateMap, nil
}

// updateTuples applies updates to all collected tuples
// UPDATE is implemented as DELETE + INSERT at the storage layer
func (p *UpdatePlan) updateTuples(tuples []*tuple.Tuple, tupleDesc *tuple.TupleDescription, updateMap map[int]types.Field) error {
	for _, old := range tuples {
		newTup, err := buildUpdatedTuple(old, tupleDesc, updateMap)
		if err != nil {
			return err
		}
		if err := p.ctx.PageStore().UpdateTuple(p.tid, old, newTup); err != nil {
			return fmt.Errorf("failed to update tuple: %v", err)
		}
	}

	return nil
}

func buildUpdatedTuple(old *tuple.Tuple, tupleDesc *tuple.TupleDescription, updateMap map[int]types.Field) (*tuple.Tuple, error) {
	fieldCnt := tupleDesc.NumFields()
	newTup := tuple.NewTuple(tupleDesc)

	for i := range fieldCnt {
		field, err := old.GetField(i)
		if err != nil {
			return nil, fmt.Errorf("failed to get field %d from old tuple: %w", i, err)
		}

		if err := newTup.SetField(i, field); err != nil {
			return nil, fmt.Errorf("failed to copy field %d: %w", i, err)
		}
	}

	for i, newValue := range updateMap {
		if err := newTup.SetField(i, newValue); err != nil {
			return nil, fmt.Errorf("failed to set updated field %d: %w", i, err)
		}
	}

	return newTup, nil
}
