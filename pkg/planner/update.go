package planner

import (
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type UpdatePlan struct {
	statement *statements.UpdateStatement
	ctx       DbContext
	tx        TransactionCtx
}

func NewUpdatePlan(statement *statements.UpdateStatement, tx TransactionCtx, ctx DbContext) *UpdatePlan {
	return &UpdatePlan{
		statement: statement,
		ctx:       ctx,
		tx:        tx,
	}
}

// Execute performs the UPDATE operation in three phases:
//  1. Validate table exists and build field index→value update map
//  2. Scan table to collect all tuples matching WHERE clause
//  3. Apply updates using storage layer's DELETE+INSERT mechanism
//
// Returns a DMLResult with the count of modified rows.
// All operations are atomic within the transaction - failures trigger rollback.
func (p *UpdatePlan) Execute() (any, error) {
	md, err := resolveTableMetadata(p.statement.TableName, p.tx.ID, p.ctx)
	if err != nil {
		return nil, err
	}

	updateMap, err := p.buildUpdateMap(md.TupleDesc)
	if err != nil {
		return nil, err
	}

	queryPlan, err := buildScanWithFilter(p.tx, md.TableID, p.statement.WhereClause, p.ctx)
	if err != nil {
		return nil, err
	}

	tuplesToUpdate, err := collectAllTuples(queryPlan)
	if err != nil {
		return nil, err
	}

	err = p.updateTuples(tuplesToUpdate, updateMap, md.TableID)
	if err != nil {
		return nil, err
	}

	return &DMLResult{
		RowsAffected: len(tuplesToUpdate),
		Message:      fmt.Sprintf("%d row(s) updated", len(tuplesToUpdate)),
	}, nil
}

// buildUpdateMap converts SET clause field names to tuple field indices.
// Validates that all referenced fields exist in the table schema.
//
// Returns a map of field_index → new_field_value for efficient updates.
func (p *UpdatePlan) buildUpdateMap(tupleDesc TupleDesc) (map[int]types.Field, error) {
	updateMap := make(map[int]types.Field)
	for _, cl := range p.statement.SetClauses {
		i, err := tupleDesc.FindFieldIndex(cl.FieldName)
		if err != nil {
			return nil, err
		}
		updateMap[i] = cl.Value
	}
	return updateMap, nil
}

// updateTuples applies updates to all collected tuples
// UPDATE is implemented as DELETE + INSERT at the storage layer
func (p *UpdatePlan) updateTuples(tuples []*tuple.Tuple, updateMap map[int]types.Field, tableID int) error {
	tupleMgr := p.ctx.TupleManager()
	ctm := p.ctx.CatalogManager()

	file, err := ctm.GetTableFile(tableID)
	if err != nil {
		return err
	}
	for _, old := range tuples {
		newTup, err := old.WithUpdatedFields(updateMap)
		if err != nil {
			return err
		}
		if err := tupleMgr.UpdateTuple(p.tx, file, old, newTup); err != nil {
			return fmt.Errorf("failed to update tuple: %v", err)
		}
	}

	return nil
}
