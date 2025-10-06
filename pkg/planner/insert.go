package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
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
	statement *statements.InsertStatement
	ctx       *registry.DatabaseContext
	tid       *transaction.TransactionID
}

// NewInsertPlan creates a new InsertPlan instance with the provided components.
// This constructor initializes the plan with all necessary dependencies for
// executing INSERT operations within a transactional context.
func NewInsertPlan(
	stmt *statements.InsertStatement,
	tid *transaction.TransactionID,
	ctx *registry.DatabaseContext) *InsertPlan {
	return &InsertPlan{
		statement: stmt,
		ctx:       ctx,
		tid:       tid,
	}
}

// Execute performs the INSERT operation by processing the statement and inserting
// all specified tuples into the target table. It validates the data, creates
// tuples according to the table schema, and coordinates with the storage layer.
//
// Returns a DMLResult containing the number of inserted rows and a success message,
// or an error if the operation fails at any stage.
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

// getTableID resolves the table name from the INSERT statement to its internal ID.
func (p *InsertPlan) getTableID() (int, error) {
	tableName := p.statement.TableName
	tableID, err := p.ctx.TableManager().GetTableID(tableName)
	if err != nil {
		return 0, fmt.Errorf("table %s not found", tableName)
	}
	return tableID, nil
}

// getTupleDesc retrieves the schema definition (tuple description) for the target table.
func (p *InsertPlan) getTupleDesc() (*tuple.TupleDescription, error) {
	tableName := p.statement.TableName
	tableID, err := p.ctx.TableManager().GetTableID(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	tupleDesc, err := p.ctx.TableManager().GetTupleDesc(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %s", tableName)
	}

	return tupleDesc, nil
}

// createFieldMapping builds a mapping between the specified field names in the INSERT
// statement and their corresponding indices in the table schema. This allows for
// INSERT statements with explicit field lists (e.g., INSERT INTO table (col1, col3) VALUES (...)).
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

// insertTuples processes each set of values in the INSERT statement, creating and
// inserting tuples into the specified table. It validates value counts, creates
// tuples according to the schema, and uses the page store for persistent storage.
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

		if err := p.ctx.PageStore().InsertTuple(p.tid, tableID, newTuple); err != nil {
			return 0, fmt.Errorf("failed to insert tuple: %v", err)
		}

		insertedCount++
	}

	return insertedCount, nil
}

// validateValueCount ensures that the number of values provided matches the expected
// number of fields, either from the explicit field list or the complete table schema.
// This prevents runtime errors during tuple creation.
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

// createTuple constructs a new tuple from the provided values according to the table schema.
// It handles both explicit field mappings (for partial inserts) and full row inserts.
// For explicit mappings, it validates that all required fields are provided.
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

// findFieldIndex locates the schema index for a given field name.
func (p *InsertPlan) findFieldIndex(fieldName string, tupleDesc *tuple.TupleDescription) (int, error) {
	for i := 0; i < tupleDesc.NumFields(); i++ {
		name, _ := tupleDesc.GetFieldName(i)
		if name == fieldName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in table %s", fieldName, p.statement.TableName)
}

// isFieldMapped checks whether a specific field index is included in the field mapping.
func isFieldMapped(fieldIndex int, fieldMapping []int) bool {
	for _, mappedIndex := range fieldMapping {
		if mappedIndex == fieldIndex {
			return true
		}
	}
	return false
}
