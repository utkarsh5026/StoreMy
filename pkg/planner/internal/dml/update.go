package dml

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/metadata"
	"storemy/pkg/planner/internal/result"
	"storemy/pkg/planner/internal/scan"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type updatedColMap = map[primitives.ColumnID]types.Field

// UpdatePlan represents a physical plan for executing an UPDATE statement.
//
// An UPDATE operation modifies existing rows in a table by:
// - Scanning the table to find rows matching the WHERE clause
// - Applying the SET clause changes to each matched row
// - Persisting the changes through the storage layer
//
// The plan holds the parsed UPDATE statement, transaction context, and database context
// needed to execute the operation.
type UpdatePlan struct {
	statement *statements.UpdateStatement
	ctx       *registry.DatabaseContext
	tx        *transaction.TransactionContext
}

// NewUpdatePlan creates a new UpdatePlan for executing an UPDATE statement.
//
// Parameters:
// - statement: the parsed UPDATE statement containing table name, SET clauses, and WHERE condition.
// - tx: the transaction context under which the update will be executed.
// - ctx: the database context providing access to catalog and storage managers.
//
// Returns:
// - *UpdatePlan: a new update plan ready for execution.
func NewUpdatePlan(statement *statements.UpdateStatement, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) *UpdatePlan {
	return &UpdatePlan{
		statement: statement,
		ctx:       ctx,
		tx:        tx,
	}
}

// Execute performs the UPDATE operation in three phases:
// 1. Validate table exists and build field index→value update map
// 2. Scan table to collect all tuples matching WHERE clause
// 3. Apply updates using storage layer's DELETE+INSERT mechanism
//
// Returns a DMLResult with the count of modified rows.
// All operations are atomic within the transaction - failures trigger rollback.
func (p *UpdatePlan) Execute() (result.Result, error) {
	md, err := metadata.ResolveTableMetadata(p.statement.TableName, p.tx, p.ctx)
	if err != nil {
		return nil, err
	}

	updateMap, err := p.buildUpdateMap(md.TupleDesc)
	if err != nil {
		return nil, err
	}

	queryPlan, err := scan.BuildScanWithFilter(p.tx, md.TableID, p.statement.WhereClause, p.ctx)
	if err != nil {
		return nil, err
	}

	tuplesToUpdate, err := metadata.CollectAllTuples(queryPlan)
	if err != nil {
		return nil, err
	}

	err = p.updateTuples(tuplesToUpdate, updateMap, md.TableID)
	if err != nil {
		return nil, err
	}

	return &result.DMLResult{
		RowsAffected: len(tuplesToUpdate),
		Message:      fmt.Sprintf("%d row(s) updated", len(tuplesToUpdate)),
	}, nil
}

// buildUpdateMap converts SET clause field names to tuple field indices.
// Validates that all referenced fields exist in the table schema.
//
// Parameters:
// - tupleDesc: the tuple description of the target table, used to resolve field names.
//
// Returns:
// - map[int]types.Field: a map of field_index → new_field_value for efficient updates.
// - error: non-nil if any field name in the SET clauses cannot be resolved.
func (p *UpdatePlan) buildUpdateMap(tupleDesc *tuple.TupleDescription) (updatedColMap, error) {
	updateMap := make(updatedColMap)
	for _, cl := range p.statement.SetClauses {
		i, err := tupleDesc.FindFieldIndex(cl.FieldName)
		if err != nil {
			return nil, err
		}
		updateMap[i] = cl.Value
	}
	return updateMap, nil
}

// updateTuples applies updates to all collected tuples.
//
// UPDATE is implemented as DELETE + INSERT at the storage layer:
// - For each tuple, a new tuple is created with updated field values
// - The old tuple is deleted and the new tuple is inserted
// - The tuple manager ensures atomicity within the transaction
//
// Parameters:
// - tuples: slice of tuples to be updated.
// - updateMap: map of field indices to new field values.
// - tableID: the identifier of the table being updated.
//
// Returns:
// - error: non-nil if any tuple update fails (e.g., constraint violation, I/O error).
func (p *UpdatePlan) updateTuples(tuples []*tuple.Tuple, updateMap updatedColMap, tableID primitives.FileID) error {
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
