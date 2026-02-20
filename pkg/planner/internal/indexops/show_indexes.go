package indexops

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/shared"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
	"sync"
)

// ShowIndexesPlan represents the execution plan for SHOW INDEXES statement.
// It retrieves index metadata from the CATALOG_INDEXES system table and returns
// it as a result set.
//
// Example:
//
//	SHOW INDEXES;                    -- Show all indexes
//	SHOW INDEXES FROM users;          -- Show indexes for 'users' table
type ShowIndexesPlan struct {
	Statement *statements.ShowIndexesStatement // Parsed SHOW INDEXES statement
	ctx       *registry.DatabaseContext        // Database context for catalog access
	tx        *transaction.TransactionContext  // Current transaction for catalog operations
}

// NewShowIndexesPlan creates a new SHOW INDEXES plan instance.
//
// Parameters:
//   - stmt: Parsed SHOW INDEXES statement containing optional table name
//   - ctx: Database context providing access to CatalogManager
//   - tx: Active transaction for catalog reads
func NewShowIndexesPlan(
	stmt *statements.ShowIndexesStatement,
	ctx *registry.DatabaseContext,
	tx *transaction.TransactionContext,
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
func (p *ShowIndexesPlan) Execute() (shared.Result, error) {
	var indexes []*systable.IndexMetadata
	var err error

	if p.Statement.TableName != "" {
		indexes, err = p.getIndexesForTable()
	} else {
		indexes, err = p.getAllIndexes()
	}

	if err != nil {
		return nil, err
	}

	tupleDesc, tuples := p.createResultTuples(indexes)
	return &shared.SelectQueryResult{
		TupleDesc: tupleDesc,
		Tuples:    tuples,
	}, nil
}

// getIndexesForTable retrieves all indexes for a specific table.
//
// This method validates that the specified table exists and fetches all associated
// index metadata from the catalog. It's used when SHOW INDEXES FROM <table> is executed.
//
// Returns:
//   - Slice of IndexMetadata pointers for the specified table
//   - Error if table doesn't exist or catalog read fails
func (p *ShowIndexesPlan) getIndexesForTable() ([]*systable.IndexMetadata, error) {
	cm := p.ctx.CatalogManager()
	tableName := p.Statement.TableName

	tableID, err := cm.GetTableID(p.tx, tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	indexes, err := cm.GetIndexesByTable(p.tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve indexes for table %s: %w", tableName, err)
	}
	return indexes, nil
}

// getAllIndexes retrieves all indexes from the catalog.
//
// Returns:
//   - Slice of all IndexMetadata pointer entries
//   - Error if catalog read fails
func (p *ShowIndexesPlan) getAllIndexes() ([]*systable.IndexMetadata, error) {
	cm := p.ctx.CatalogManager()
	return cm.GetAllIndexes(p.tx)
}

// createResultTuples converts index metadata into displayable tuples.
//
// Parameters:
//   - indexes: Slice of IndexMetadata pointers to convert
//
// Returns:
//   - TupleDescription for the result schema
//   - Slice of tuples containing index information
func (p *ShowIndexesPlan) createResultTuples(indexes []*systable.IndexMetadata) (*tuple.TupleDescription, []*tuple.Tuple) {
	sch := p.createIndexSchema()
	tuples := make([]*tuple.Tuple, 0, len(indexes))

	tupChan := make(chan *tuple.Tuple, len(indexes))

	var wg sync.WaitGroup
	for _, idx := range indexes {
		wg.Add(1)
		go func(idx *systable.IndexMetadata) {
			defer wg.Done()
			p.createIndexTuple(*idx, sch.TupleDesc, tupChan)
		}(idx)
	}

	wg.Wait()
	close(tupChan)

	for t := range tupChan {
		tuples = append(tuples, t)
	}
	return sch.TupleDesc, tuples
}

func (p *ShowIndexesPlan) createIndexSchema() *schema.Schema {
	sch, _ := schema.NewSchemaBuilder(systable.InvalidTableID, "show_indexes_result").
		AddColumn("index_name", types.StringType).
		AddColumn("table_name", types.StringType).
		AddColumn("column_name", types.StringType).
		AddColumn("index_type", types.StringType).
		AddColumn("created_at", types.IntType).
		Build()
	return sch
}

func (p *ShowIndexesPlan) createIndexTuple(idx systable.IndexMetadata, td *tuple.TupleDescription, tupChan chan<- *tuple.Tuple) {
	tableName, err := p.ctx.CatalogManager().GetTableName(p.tx, idx.TableID)
	if err != nil {
		tableName = fmt.Sprintf("table_%d", idx.TableID)
	}

	t := tuple.NewBuilder(td).
		AddString(idx.IndexName).
		AddString(tableName).
		AddString(idx.ColumnName).
		AddString(strings.ToUpper(string(idx.IndexType))).
		AddTimestamp(idx.CreatedAt).
		MustBuild()

	tupChan <- t
}
