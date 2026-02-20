package dml

import (
	"fmt"
	"slices"
	"storemy/pkg/catalog/systable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/shared"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type columnIndexMapping = []primitives.ColumnID

// InsertPlan represents an execution plan for an INSERT statement.
// It encapsulates all the necessary components to execute an INSERT operation
// including the parsed statement, database context, and transaction context.
//
// The plan supports:
//   - Full row inserts: INSERT INTO table VALUES (...)
//   - Partial column inserts: INSERT INTO table (col1, col2) VALUES (...)
//   - Auto-increment columns: Automatically generates values for auto-increment fields
//   - Batch inserts: Multiple value sets in a single INSERT statement
//
// Example usage:
//
//	plan := NewInsertPlan(stmt, tx, ctx)
//	result, err := plan.Execute()
type InsertPlan struct {
	statement *statements.InsertStatement     // The parsed INSERT statement to execute
	ctx       *registry.DatabaseContext       // Database-wide context and managers
	tx        *transaction.TransactionContext // Transaction context for ACID compliance
}

// NewInsertPlan creates a new InsertPlan instance with the provided components.
// This constructor initializes the plan with all necessary dependencies for
// executing INSERT systable within a transactional context.
//
// Parameters:
//   - stmt: The parsed INSERT statement containing table name, fields, and values
//   - tx: Transaction context for ensuring ACID properties
//   - ctx: Database context providing access to catalog and tuple managers
//
// Returns:
//   - A fully initialized InsertPlan ready for execution
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
//
// The execution flow:
//  1. Resolves table metadata from the catalog
//  2. Builds column index mapping if explicit fields are specified
//  3. Retrieves auto-increment information if applicable
//  4. Inserts each tuple and updates auto-increment counter
//  5. Returns the number of rows affected
//
// Returns:
//   - A DMLResult containing the number of rows inserted and a success message
//   - An error if table doesn't exist, validation fails, or insertion fails
//
// Errors:
//   - Table not found
//   - Invalid column names
//   - Value count mismatch
//   - Type conversion errors
//   - Storage layer errors
func (p *InsertPlan) Execute() (shared.Result, error) {
	md, err := shared.ResolveTableMetadata(p.statement.TableName, p.tx, p.ctx)
	if err != nil {
		return nil, err
	}

	mapping, err := p.buildColumnIndexMapping(md.TupleDesc)
	if err != nil {
		return nil, err
	}

	autoIncInfo, err := p.ctx.CatalogManager().ColumnTable.GetAutoIncrementColumn(p.tx, md.TableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get auto-increment info: %v", err)
	}

	insertedCount, err := p.insertTuples(md.TableID, md.TupleDesc, mapping, autoIncInfo)
	if err != nil {
		return nil, err
	}

	return &shared.DMLResult{
		RowsAffected: insertedCount,
		Message:      fmt.Sprintf("%d row(s) inserted", insertedCount),
	}, nil
}

// buildColumnIndexMapping builds a mapping between the specified field names in the INSERT
// statement and their corresponding indices in the table schema. This allows for
// INSERT statements with explicit field lists (e.g., INSERT INTO table (col1, col3) VALUES (...)).
//
// If no explicit fields are specified in the INSERT statement, returns nil to indicate
// that values should be mapped sequentially to all table columns.
//
// Parameters:
//   - td: The tuple description containing the table schema
//
// Returns:
//   - A slice where each element is the table column index for the corresponding value position
//   - nil if no explicit field list was provided
//   - An error if any specified field name doesn't exist in the table
//
// Example:
//
//	For INSERT INTO users (email, name) VALUES (...) where table has [id, name, email, age]
//	Returns: [2, 1] (email is index 2, name is index 1)
func (p *InsertPlan) buildColumnIndexMapping(td *tuple.TupleDescription) (columnIndexMapping, error) {
	if len(p.statement.Fields) == 0 {
		return nil, nil
	}

	fieldNames := p.statement.Fields
	mapping := make([]primitives.ColumnID, len(fieldNames))
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
//
// For each value set, it:
//  1. Validates that the number of values matches expectations
//  2. Creates a tuple with proper field placement
//  3. Handles auto-increment column generation
//  4. Inserts the tuple through the tuple manager
//  5. Updates the auto-increment counter if applicable
//
// Parameters:
//   - tableID: The unique identifier of the target table
//   - tupleDesc: The schema description of the table
//   - fieldMapping: Optional mapping of value positions to column indices (nil for sequential)
//   - autoIncInfo: Information about auto-increment column (nil if none exists)
//
// Returns:
//   - The number of successfully inserted tuples
//   - An error if validation fails or any insertion fails (all inserts are rolled back)
func (p *InsertPlan) insertTuples(tableID primitives.FileID, tupleDesc *tuple.TupleDescription, fieldMapping columnIndexMapping, autoIncInfo *systable.AutoIncrementInfo) (int, error) {
	cm := p.ctx.CatalogManager()
	dbFile, err := cm.GetTableFile(tableID)
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
			if err := p.ctx.CatalogManager().ColumnTable.IncrementAutoIncrementValue(p.tx, tableID, autoIncInfo.ColumnName, newValue); err != nil {
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
//
// When an auto-increment column exists and is not explicitly provided in the field mapping,
// the function expects one fewer value since the auto-increment value is generated automatically.
//
// Parameters:
//   - values: The slice of field values to be inserted
//   - td: The tuple description defining the table schema
//   - fieldMapping: Optional explicit field list (nil means all fields in order)
//   - autoIncInfo: Information about auto-increment column (nil if none exists)
//
// Returns:
//   - nil if value count is correct
//   - An error describing the mismatch if value count is incorrect
//
// Examples:
//
//	Table: [id (auto), name, email]
//	INSERT INTO table VALUES ('John', 'john@example.com') - expects 2 values
//	INSERT INTO table (name, email) VALUES ('John', 'john@example.com') - expects 2 values
//	INSERT INTO table (id, name, email) VALUES (1, 'John', 'john@example.com') - expects 3 values
func validateValueCount(values []types.Field, td *tuple.TupleDescription, fieldMapping columnIndexMapping, autoIncInfo *systable.AutoIncrementInfo) error {
	var expected primitives.ColumnID
	if fieldMapping != nil {
		expected = primitives.ColumnID(len(fieldMapping)) // #nosec G115
	} else {
		expected = td.NumFields()
		if autoIncInfo != nil {
			expected--
		}
	}

	if len(values) != int(expected) {
		return fmt.Errorf("value count mismatch: expected %d, got %d", expected, len(values))
	}

	return nil
}

// createTuple constructs a new tuple from the provided values according to the table schema.
// It handles both explicit field mappings (for partial inserts) and full row inserts.
// For explicit mappings, it validates that all required fields are provided.
// If auto-increment info is provided, it automatically fills the auto-increment column.
//
// This is a dispatcher function that delegates to specialized builders based on
// whether an explicit field mapping is provided.
//
// Parameters:
//   - values: The field values to insert
//   - tupleDesc: The schema definition of the table
//   - fieldMapping: Optional mapping of values to specific columns (nil for sequential)
//   - autoIncInfo: Information about auto-increment column (nil if none exists)
//
// Returns:
//   - A fully constructed tuple ready for insertion
//   - An error if tuple construction fails (type mismatch, missing fields, etc.)
func createTuple(values []types.Field, tupleDesc *tuple.TupleDescription, fieldMapping columnIndexMapping, autoIncInfo *systable.AutoIncrementInfo) (*tuple.Tuple, error) {
	if fieldMapping != nil {
		return buildTupleWithPartialColumns(values, tupleDesc, fieldMapping, autoIncInfo)
	}
	return buildTupleFromAllColumns(values, tupleDesc, autoIncInfo)
}

// buildTupleWithPartialColumns constructs a tuple when an explicit field list is provided.
// This handles INSERT statements like: INSERT INTO table (col1, col3) VALUES (val1, val3)
//
// The function:
//  1. Creates an empty tuple
//  2. Populates fields according to the mapping
//  3. Adds auto-increment value if the auto-increment column wasn't explicitly provided
//  4. Validates that all required columns have values
//
// Parameters:
//   - values: The field values in the order specified in the INSERT statement
//   - td: The tuple description defining the complete table schema
//   - mapping: Array mapping value indices to table column indices
//   - autoInc: Information about auto-increment column (nil if none exists)
//
// Returns:
//   - A complete tuple with all fields populated
//   - An error if any required field is missing or field population fails
//
// Example:
//
//	Table schema: [id (auto), name, email, age]
//	INSERT INTO table (email, name) VALUES ('john@example.com', 'John')
//	mapping: [2, 1] (email=index 2, name=index 1)
//	Result: tuple with id=auto, name='John', email='john@example.com', age=error (missing)
func buildTupleWithPartialColumns(values []types.Field, td *tuple.TupleDescription, mapping columnIndexMapping, autoInc *systable.AutoIncrementInfo) (*tuple.Tuple, error) {
	newTuple := tuple.NewTuple(td)
	if err := populateMappedColumns(newTuple, values, mapping); err != nil {
		return nil, err
	}

	if autoInc != nil && !slices.Contains(mapping, autoInc.ColumnIndex) {
		if err := applyAutoIncrementValue(newTuple, autoInc); err != nil {
			return nil, err
		}
	}

	if err := ensureAllColumnsProvided(td, mapping, autoInc); err != nil {
		return nil, err
	}

	return newTuple, nil
}

// buildTupleFromAllColumns constructs a tuple when no explicit field list is provided.
// This handles INSERT INTO table VALUES (val1, val2, val3)
//
// Values are assigned sequentially to table columns, skipping any auto-increment column
// which is automatically generated. The function ensures values align with the table
// schema after accounting for the auto-increment column.
//
// Parameters:
//   - values: The field values in sequential order
//   - td: The tuple description defining the complete table schema
//   - autoInc: Information about auto-increment column (nil if none exists)
//
// Returns:
//   - A complete tuple with all fields populated
//   - An error if there are too few values or field assignment fails
//
// Example:
//
//	Table schema: [id (auto), name, email, age]
//	INSERT INTO table VALUES ('John', 'john@example.com', 25)
//	Result: tuple with id=auto, name='John', email='john@example.com', age=25
func buildTupleFromAllColumns(values []types.Field, td *tuple.TupleDescription, autoInc *systable.AutoIncrementInfo) (*tuple.Tuple, error) {
	newTuple := tuple.NewTuple(td)
	valueIndex := 0
	var i primitives.ColumnID
	for i = 0; i < td.NumFields(); i++ {
		if autoInc != nil && i == autoInc.ColumnIndex {
			if err := applyAutoIncrementValue(newTuple, autoInc); err != nil {
				return nil, err
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

// applyAutoIncrementValue sets the auto-increment field in a tuple to its next value.
// This is called automatically during tuple construction to populate auto-increment columns.
//
// Parameters:
//   - t: The tuple to modify
//   - autoInc: Contains the column index and next value to use
//
// Returns:
//   - nil on success
//   - An error if setting the field fails (wrong type, invalid index, etc.)
func applyAutoIncrementValue(t *tuple.Tuple, autoInc *systable.AutoIncrementInfo) error {
	index := autoInc.ColumnIndex
	value := types.NewIntField(int64(autoInc.NextValue)) // #nosec G115
	if err := t.SetField(index, value); err != nil {
		return fmt.Errorf("failed to set auto-increment field: %v", err)
	}
	return nil
}

// populateMappedColumns sets tuple fields using an explicit field mapping.
// Used when INSERT statement specifies a subset of columns.
//
// Each value is placed in the tuple at the position specified by the corresponding
// mapping index. This allows for flexible column ordering in INSERT statements.
//
// Parameters:
//   - tup: The tuple to populate
//   - values: The field values to insert
//   - mp: Maps each value index to its target column index in the tuple
//
// Returns:
//   - nil on success
//   - An error if any field assignment fails (type mismatch, invalid index, etc.)
//
// Example:
//
//	fieldMapping: [2, 0, 3]
//	values: [val0, val1, val2]
//	Result: tuple[0]=val1, tuple[2]=val0, tuple[3]=val2
func populateMappedColumns(tup *tuple.Tuple, values []types.Field, mp columnIndexMapping) error {
	for i, value := range values {
		if err := tup.SetField(mp[i], value); err != nil {
			return fmt.Errorf("failed to set field at index %d: %w", mp[i], err)
		}
	}
	return nil
}

// ensureAllColumnsProvided ensures all table fields have values when using explicit field mapping.
// Prevents NULL values in fields not included in the INSERT field list.
// Auto-increment columns are exempt from this check as they are auto-filled.
//
// This enforces that partial column inserts must still provide values for all non-auto-increment
// columns. In the future, this could be relaxed to allow NULL values or default values.
//
// Parameters:
//   - td: The complete table schema
//   - fieldMapping: The explicitly provided column indices
//   - autoInc: Information about auto-increment column (nil if none exists)
//
// Returns:
//   - nil if all required columns are present in the mapping
//   - An error specifying which field index is missing
//
// Example:
//
//	Table: [id (auto), name, email, age]
//	Mapping: [1, 2] (name, email provided)
//	Result: Error - missing value for field index 3 (age)
func ensureAllColumnsProvided(td *tuple.TupleDescription, fieldMapping columnIndexMapping, autoInc *systable.AutoIncrementInfo) error {
	var i primitives.ColumnID
	for i = 0; i < td.NumFields(); i++ {
		if autoInc != nil && i == autoInc.ColumnIndex {
			continue
		}

		if !slices.Contains(fieldMapping, i) {
			return fmt.Errorf("missing value for field index %d", i)
		}
	}
	return nil
}
