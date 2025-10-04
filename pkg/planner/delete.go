package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/parser/statements"
	"storemy/pkg/tuple"
)

// DeletePlan implements the execution plan for DELETE statements.
// It handles the complete lifecycle of a delete operation including:
// - Table identification and validation
// - Query plan construction with optional WHERE filtering
// - Tuple collection and deletion
// - Transaction management
type DeletePlan struct {
	statement    *statements.DeleteStatement // Parsed DELETE statement
	pageStore    *memory.PageStore           // Storage manager for tuple operations
	tableManager *memory.TableManager        // Manager for table metadata
	tid          *transaction.TransactionID  // Transaction context for the operation
}

// NewDeletePlan creates a new DELETE execution plan.
//
// Parameters:
//   - stmt: Parsed DELETE statement containing table name and WHERE clause
//   - ps: PageStore instance for tuple storage operations
//   - tid: Transaction ID for ACID compliance
//   - tm: TableManager for table metadata operations
//
// Returns:
//   - *DeletePlan: New delete plan instance ready for execution
func NewDeletePlan(
	stmt *statements.DeleteStatement,
	ps *memory.PageStore,
	tid *transaction.TransactionID,
	tm *memory.TableManager) *DeletePlan {
	return &DeletePlan{
		statement:    stmt,
		pageStore:    ps,
		tableManager: tm,
		tid:          tid,
	}
}

// Execute performs the DELETE operation following these steps:
// 1. Resolve table name to table ID
// 2. Build query plan with optional WHERE filtering
// 3. Collect all tuples matching the criteria
// 4. Delete the collected tuples
//
// Returns:
//   - any: DMLResult containing affected row count and success message
//   - error: nil on success, error describing failure reason
func (p *DeletePlan) Execute() (any, error) {
	tableID, err := p.getTableID()
	if err != nil {
		return nil, err
	}

	query, err := p.createQuery(tableID)
	if err != nil {
		return nil, err
	}

	tuplesToDelete, err := collectTuplesToDelete(query)
	if err != nil {
		return nil, err
	}

	err = p.deleteTuples(tuplesToDelete)
	if err != nil {
		return nil, err
	}

	return &DMLResult{
		RowsAffected: len(tuplesToDelete),
		Message:      fmt.Sprintf("%d row(s) deleted", len(tuplesToDelete)),
	}, nil
}

// getTableID resolves the table name from the DELETE statement to its internal table ID.
//
// Returns:
//   - int: Table ID for the specified table
//   - error: nil on success, error if table doesn't exist
func (p *DeletePlan) getTableID() (int, error) {
	tableName := p.statement.TableName
	tableID, err := p.tableManager.GetTableID(tableName)
	if err != nil {
		return 0, fmt.Errorf("table %s not found", tableName)
	}
	return tableID, nil
}

// createTableScan creates a sequential scan iterator for the target table.
func (p *DeletePlan) createTableScan(tableID int) (iterator.DbIterator, error) {
	scanOp, err := query.NewSeqScan(p.tid, tableID, p.tableManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %v", err)
	}
	return scanOp, nil
}

// addWhereFilter wraps the scan iterator with a filter based on the WHERE clause.
func (p *DeletePlan) addWhereFilter(scanOp iterator.DbIterator) (iterator.DbIterator, error) {
	predicate, err := buildPredicateFromFilterNode(p.statement.WhereClause, scanOp.GetTupleDesc())
	if err != nil {
		return nil, fmt.Errorf("failed to build WHERE predicate: %v", err)
	}

	filterOp, err := query.NewFilter(predicate, scanOp)
	if err != nil {
		return nil, fmt.Errorf("failed to create filter: %v", err)
	}

	return filterOp, nil
}

// collectTuplesToDelete executes the query plan and collects all tuples that match the criteria.
// This approach ensures we have all target tuples before beginning deletion to avoid
// iterator invalidation during modification.
func collectTuplesToDelete(queryPlan iterator.DbIterator) ([]*tuple.Tuple, error) {
	if err := queryPlan.Open(); err != nil {
		return nil, fmt.Errorf("failed to open delete query: %v", err)
	}
	defer queryPlan.Close()

	var tuplesToDelete []*tuple.Tuple

	for {
		hasNext, err := queryPlan.HasNext()
		if err != nil {
			return nil, fmt.Errorf("error during delete scan: %v", err)
		}

		if !hasNext {
			break
		}

		tuple, err := queryPlan.Next()
		if err != nil {
			return nil, fmt.Errorf("error fetching tuple to delete: %v", err)
		}

		tuplesToDelete = append(tuplesToDelete, tuple)
	}

	return tuplesToDelete, nil
}

// deleteTuples performs the actual deletion of collected tuples.
func (p *DeletePlan) deleteTuples(tuplesToDelete []*tuple.Tuple) error {
	for i, tupleToDelete := range tuplesToDelete {
		if err := p.pageStore.DeleteTuple(p.tid, tupleToDelete); err != nil {
			return fmt.Errorf("failed to delete tuple %d: %v", i+1, err)
		}
	}
	return nil
}

// createQuery builds the complete query execution plan for the DELETE operation.
// The plan consists of:
// - A sequential scan of the target table
// - An optional filter if WHERE clause is present
func (p *DeletePlan) createQuery(tableID int) (iterator.DbIterator, error) {
	scanOp, err := p.createTableScan(tableID)
	if err != nil {
		return nil, err
	}

	var currentOp iterator.DbIterator
	currentOp = scanOp

	if p.statement.WhereClause != nil {
		filter, err := p.addWhereFilter(scanOp)
		if err != nil {
			return nil, err
		}
		currentOp = filter
	}

	return currentOp, nil
}
