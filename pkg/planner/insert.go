package planner

import (
	"fmt"
	"slices"
	"storemy/pkg/parser/statements"
	"storemy/pkg/primitives"
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
	tid       *primitives.TransactionID
}

// NewInsertPlan creates a new InsertPlan instance with the provided components.
// This constructor initializes the plan with all necessary dependencies for
// executing INSERT operations within a transactional context.
func NewInsertPlan(
	stmt *statements.InsertStatement,
	tid *primitives.TransactionID,
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
	md, err := resolveTableMetadata(p.statement.TableName, p.ctx)
	if err != nil {
		return nil, err
	}

	mapping, err := p.createFieldMapping(md.TupleDesc)
	if err != nil {
		return nil, err
	}

	insertedCount, err := p.insertTuples(md.TableID, md.TupleDesc, mapping)
	if err != nil {
		return nil, err
	}

	return &DMLResult{
		RowsAffected: insertedCount,
		Message:      fmt.Sprintf("%d row(s) inserted", insertedCount),
	}, nil
}

// createFieldMapping builds a mapping between the specified field names in the INSERT
// statement and their corresponding indices in the table schema. This allows for
// INSERT statements with explicit field lists (e.g., INSERT INTO table (col1, col3) VALUES (...)).
func (p *InsertPlan) createFieldMapping(td *tuple.TupleDescription) ([]int, error) {
	if len(p.statement.Fields) == 0 {
		return nil, nil
	}

	fieldNames := p.statement.Fields
	mapping := make([]int, len(fieldNames))
	for i, field := range fieldNames {
		fieldIndex, err := findFieldIndex(field, td)
		if err != nil {
			return nil, err
		}
		mapping[i] = fieldIndex
	}

	return mapping, nil
}

// insertTuples processes each set of values in the INSERT statement, creating and
// inserting tuples into the specified table. It validates value counts, creates
// tuples according to the schema, and uses the page store for persistent storage.
func (p *InsertPlan) insertTuples(tableID int, tupleDesc *tuple.TupleDescription, fieldMapping []int) (int, error) {
	insertedCount := 0
	for _, values := range p.statement.Values {
		if err := validateValueCount(values, tupleDesc, fieldMapping); err != nil {
			return 0, err
		}

		newTuple, err := createTuple(values, tupleDesc, fieldMapping)
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
func validateValueCount(values []types.Field, tupleDesc *tuple.TupleDescription, fieldMapping []int) error {
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
func createTuple(values []types.Field, tupleDesc *tuple.TupleDescription, fieldMapping []int) (*tuple.Tuple, error) {
	newTuple := tuple.NewTuple(tupleDesc)
	if fieldMapping != nil {
		if err := setMappedFields(newTuple, values, fieldMapping); err != nil {
			return nil, err
		}

		if err := validateAllFieldsSet(tupleDesc, fieldMapping); err != nil {
			return nil, err
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

// setMappedFields sets tuple fields using an explicit field mapping.
// Used when INSERT statement specifies a subset of columns.
func setMappedFields(tup *tuple.Tuple, values []types.Field, fieldMapping []int) error {
	for i, value := range values {
		if err := tup.SetField(fieldMapping[i], value); err != nil {
			return fmt.Errorf("failed to set field at index %d: %w", fieldMapping[i], err)
		}
	}
	return nil
}

// validateAllFieldsSet ensures all table fields have values when using explicit field mapping.
// Prevents NULL values in fields not included in the INSERT field list.
func validateAllFieldsSet(tupleDesc *tuple.TupleDescription, fieldMapping []int) error {
	for i := 0; i < tupleDesc.NumFields(); i++ {
		if !slices.Contains(fieldMapping, i) {
			return fmt.Errorf("missing value for field index %d", i)
		}
	}
	return nil
}
