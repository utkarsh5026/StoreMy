package planner

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/parser/statements"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

// ShowIndexesPlan represents the execution plan for SHOW INDEXES statement.
// It retrieves index metadata from the CATALOG_INDEXES system table and returns
// it as a result set.
//
// Execution flow:
//  1. Validates optional table name if specified
//  2. Retrieves index metadata from catalog
//  3. Filters by table name if specified
//  4. Converts metadata to tuples for display
//  5. Returns SelectQueryResult with index information
//
// Transaction Semantics:
//   - Read-only operation (uses shared locks on CATALOG_INDEXES)
//   - No modifications to catalog or disk
//
// Example:
//
//	SHOW INDEXES;                    -- Show all indexes
//	SHOW INDEXES FROM users;          -- Show indexes for 'users' table
type ShowIndexesPlan struct {
	Statement *statements.ShowIndexesStatement // Parsed SHOW INDEXES statement
	ctx       DbContext                        // Database context for catalog access
	tx        TxContext                        // Current transaction for catalog operations
}

// NewShowIndexesPlan creates a new SHOW INDEXES plan instance.
//
// Parameters:
//   - stmt: Parsed SHOW INDEXES statement containing optional table name
//   - ctx: Database context providing access to CatalogManager
//   - tx: Active transaction for catalog reads
//
// Returns:
//
//	Plan ready for execution via Execute() method
func NewShowIndexesPlan(
	stmt *statements.ShowIndexesStatement,
	ctx DbContext,
	tx TxContext,
) *ShowIndexesPlan {
	return &ShowIndexesPlan{
		Statement: stmt,
		ctx:       ctx,
		tx:        tx,
	}
}

// Execute retrieves and returns index metadata from the catalog.
//
// Steps:
//  1. If table name specified, validates table exists and gets table ID
//  2. Retrieves index metadata (all or filtered by table)
//  3. Converts metadata to display tuples
//  4. Returns SelectQueryResult with formatted index information
//
// Returns:
//   - SelectQueryResult with index metadata on success
//   - Error if table doesn't exist or catalog read fails
func (p *ShowIndexesPlan) Execute() (Result, error) {
	cm := p.ctx.CatalogManager()
	var indexes []*systemtable.IndexMetadata
	var err error

	if p.Statement.TableName != "" {
		tableName := p.Statement.TableName

		tableID, err := cm.GetTableID(p.tx, tableName)
		if err != nil {
			return nil, fmt.Errorf("table %s does not exist", tableName)
		}

		indexes, err = cm.GetIndexesByTable(p.tx, tableID)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve indexes for table %s: %w", tableName, err)
		}
	} else {
		// Get all indexes by scanning the entire CATALOG_INDEXES table
		indexes, err = p.getAllIndexes()
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve indexes: %w", err)
		}
	}

	tupleDesc, tuples := p.createResultTuples(indexes)

	return &SelectQueryResult{
		TupleDesc: tupleDesc,
		Tuples:    tuples,
	}, nil
}

// getAllIndexes retrieves all indexes from the catalog.
//
// Returns:
//   - Slice of all IndexMetadata entries
//   - Error if catalog read fails
func (p *ShowIndexesPlan) getAllIndexes() ([]*systemtable.IndexMetadata, error) {
	cm := p.ctx.CatalogManager()
	return cm.GetAllIndexes(p.tx)
}

// createResultTuples converts index metadata into displayable tuples.
//
// Result schema:
//   - index_name STRING
//   - table_name STRING
//   - column_name STRING
//   - index_type STRING
//   - created_at INT
//
// Parameters:
//   - indexes: Slice of IndexMetadata to convert
//
// Returns:
//   - TupleDescription for the result schema
//   - Slice of tuples containing index information
func (p *ShowIndexesPlan) createResultTuples(indexes []*systemtable.IndexMetadata) (*tuple.TupleDescription, []*tuple.Tuple) {
	// Create schema for result
	sch, _ := schema.NewSchemaBuilder(-1, "show_indexes_result").
		AddColumn("index_name", types.StringType).
		AddColumn("table_name", types.StringType).
		AddColumn("column_name", types.StringType).
		AddColumn("index_type", types.StringType).
		AddColumn("created_at", types.IntType).
		Build()

	tupleDesc := sch.TupleDesc

	// Convert indexes to tuples
	var tuples []*tuple.Tuple
	cm := p.ctx.CatalogManager()

	for _, idx := range indexes {
		// Get table name from table ID
		tableName, err := cm.GetTableName(p.tx, idx.TableID)
		if err != nil {
			// If we can't get table name, use the ID
			tableName = fmt.Sprintf("table_%d", idx.TableID)
		}

		// Create tuple with index information
		t := tuple.NewBuilder(tupleDesc).
			AddString(idx.IndexName).
			AddString(tableName).
			AddString(idx.ColumnName).
			AddString(strings.ToUpper(string(idx.IndexType))).
			AddInt(idx.CreatedAt).
			MustBuild()

		tuples = append(tuples, t)
	}

	return tupleDesc, tuples
}
