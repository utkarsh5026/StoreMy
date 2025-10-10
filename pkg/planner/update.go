package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type UpdatePlan struct {
	statement *statements.UpdateStatement
	ctx       *registry.DatabaseContext
	tx        *transaction.TransactionContext
}

func NewUpdatePlan(statement *statements.UpdateStatement, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) *UpdatePlan {
	return &UpdatePlan{
		statement: statement,
		ctx:       ctx,
		tx:        tx,
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

	queryPlan, err := buildScanWithFilter(p.tx.ID, md.TableID, p.statement.WhereClause, p.ctx)
	if err != nil {
		return nil, err
	}

	tuplesToUpdate, err := collectAllTuples(queryPlan)
	if err != nil {
		return nil, err
	}

	err = p.updateTuples(tuplesToUpdate, updateMap)
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
func (p *UpdatePlan) updateTuples(tuples []*tuple.Tuple, updateMap map[int]types.Field) error {
	for _, old := range tuples {
		newTup, err := old.WithUpdatedFields(updateMap)
		if err != nil {
			return err
		}
		if err := p.ctx.PageStore().UpdateTuple(p.tx, old, newTup); err != nil {
			return fmt.Errorf("failed to update tuple: %v", err)
		}
	}

	return nil
}
