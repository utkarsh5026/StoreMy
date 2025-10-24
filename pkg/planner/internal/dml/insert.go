package dml

import (
	"fmt"
	"slices"
	"storemy/pkg/catalog/operations"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/metadata"
	"storemy/pkg/planner/internal/result"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type InsertPlan struct {
	statement *statements.InsertStatement
	ctx       *registry.DatabaseContext
	tx        *transaction.TransactionContext
}

// NewInsertPlan creates a new InsertPlan instance with the provided components.
// This constructor initializes the plan with all necessary dependencies for
// executing INSERT operations within a transactional context.
func NewInsertPlan(stmt *statements.InsertStatement, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) *InsertPlan {
	return &InsertPlan{
		statement: stmt,
		ctx:       ctx,
		tx:        tx,
	}
}

// Execute performs the INSERT operation by processing the statement and inserting
// all specified tuples into the target table. It validates the data, creates
// tuples according to the table schema, and coordinates with the storage layer.
func (p *InsertPlan) Execute() (result.Result, error) {
	md, err := metadata.ResolveTableMetadata(p.statement.TableName, p.tx, p.ctx)
	if err != nil {
		return nil, err
	}

	mapping, err := p.createFieldMapping(md.TupleDesc)
	if err != nil {
		return nil, err
	}

	// Check for auto-increment column
	autoIncInfo, err := p.ctx.CatalogManager().GetAutoIncrementColumn(p.tx, md.TableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get auto-increment info: %v", err)
	}

	insertedCount, err := p.insertTuples(md.TableID, md.TupleDesc, mapping, autoIncInfo)
	if err != nil {
		return nil, err
	}

	return &result.DMLResult{
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
		fieldIndex, err := td.FindFieldIndex(field)
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
func (p *InsertPlan) insertTuples(tableID int, tupleDesc *tuple.TupleDescription, fieldMapping []int, autoIncInfo *operations.AutoIncrementInfo) (int, error) {
	// Get the DbFile for the table
	dbFile, err := p.ctx.CatalogManager().GetTableFile(tableID)
	if err != nil {
		return 0, fmt.Errorf("failed to get table file: %v", err)
	}

	insertedCount := 0
	for _, values := range p.statement.Values {
		if err := validateValueCount(values, tupleDesc, fieldMapping, autoIncInfo); err != nil {
			return 0, err
		}

		newTuple, err := createTuple(values, tupleDesc, fieldMapping, autoIncInfo)
		if err != nil {
			return 0, err
		}

		if err := p.ctx.TupleManager().InsertTuple(p.tx, dbFile, newTuple); err != nil {
			return 0, fmt.Errorf("failed to insert tuple: %v", err)
		}

		// Update auto-increment counter if column is auto-incremented
		if autoIncInfo != nil {
			newValue := autoIncInfo.NextValue + 1
			if err := p.ctx.CatalogManager().IncrementAutoIncrementValue(p.tx, tableID, autoIncInfo.ColumnName, newValue); err != nil {
				return 0, fmt.Errorf("failed to update auto-increment value: %v", err)
			}
			autoIncInfo.NextValue = newValue
		}

		insertedCount++
	}

	return insertedCount, nil
}

// validateValueCount ensures that the number of values provided matches the expected
// number of fields, either from the explicit field list or the complete table schema.
// This prevents runtime errors during tuple creation.
// If auto-increment column exists and is not in the field mapping, we expect one fewer value.
func validateValueCount(values []types.Field, tupleDesc *tuple.TupleDescription, fieldMapping []int, autoIncInfo *operations.AutoIncrementInfo) error {
	var expected int
	if fieldMapping != nil {
		expected = len(fieldMapping)
	} else {
		expected = tupleDesc.NumFields()
		// If no field mapping and auto-increment exists, user can omit auto-increment field
		if autoIncInfo != nil {
			expected--
		}
	}

	if len(values) != expected {
		return fmt.Errorf("value count mismatch: expected %d, got %d", expected, len(values))
	}

	return nil
}

// createTuple constructs a new tuple from the provided values according to the table schema.
// It handles both explicit field mappings (for partial inserts) and full row inserts.
// For explicit mappings, it validates that all required fields are provided.
// If auto-increment info is provided, it automatically fills the auto-increment column.
func createTuple(values []types.Field, tupleDesc *tuple.TupleDescription, fieldMapping []int, autoIncInfo *operations.AutoIncrementInfo) (*tuple.Tuple, error) {
	newTuple := tuple.NewTuple(tupleDesc)

	if fieldMapping != nil {
		if err := setMappedFields(newTuple, values, fieldMapping); err != nil {
			return nil, err
		}

		// Set auto-increment value if not provided in field mapping
		if autoIncInfo != nil && !slices.Contains(fieldMapping, autoIncInfo.ColumnIndex) {
			if err := newTuple.SetField(autoIncInfo.ColumnIndex, types.NewIntField(int64(autoIncInfo.NextValue))); err != nil {
				return nil, fmt.Errorf("failed to set auto-increment field: %v", err)
			}
		}

		if err := validateAllFieldsSet(tupleDesc, fieldMapping, autoIncInfo); err != nil {
			return nil, err
		}

		return newTuple, nil
	}

	// No field mapping - insert all values in order
	valueIndex := 0
	for i := 0; i < tupleDesc.NumFields(); i++ {
		// Skip auto-increment column if present - it will be filled automatically
		if autoIncInfo != nil && i == autoIncInfo.ColumnIndex {
			if err := newTuple.SetField(i, types.NewIntField(int64(autoIncInfo.NextValue))); err != nil {
				return nil, fmt.Errorf("failed to set auto-increment field: %v", err)
			}
			continue
		}

		if valueIndex >= len(values) {
			return nil, fmt.Errorf("not enough values provided")
		}

		if err := newTuple.SetField(i, values[valueIndex]); err != nil {
			return nil, fmt.Errorf("failed to set field: %v", err)
		}
		valueIndex++
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
// Auto-increment columns are exempt from this check as they are auto-filled.
func validateAllFieldsSet(tupleDesc *tuple.TupleDescription, fieldMapping []int, autoIncInfo *operations.AutoIncrementInfo) error {
	for i := 0; i < tupleDesc.NumFields(); i++ {
		// Skip auto-increment column - it's automatically filled
		if autoIncInfo != nil && i == autoIncInfo.ColumnIndex {
			continue
		}

		if !slices.Contains(fieldMapping, i) {
			return fmt.Errorf("missing value for field index %d", i)
		}
	}
	return nil
}
