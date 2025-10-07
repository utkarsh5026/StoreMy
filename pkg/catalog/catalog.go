package catalog

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

const (
	CatalogTable           = "CATALOG_TABLES"
	ColumnsTable           = "CATALOG_COLUMNS"
	CatalogTableFileName   = "catalog_tables.dat"
	CatalogColumnsFileName = "catalog_columns.dat"
)

// SystemCatalog manages database metadata including table and column definitions.
// It maintains two system tables:
//   - CATALOG_TABLES: stores table metadata (ID, name, file path, primary key)
//   - CATALOG_COLUMNS: stores column metadata (table ID, name, type, position, is_primary)
type SystemCatalog struct {
	store          *memory.PageStore
	tableManager   *memory.TableManager
	loader         *SchemaLoader
	tablesTableID  int
	columnsTableID int
}

// NewSystemCatalog creates a new system catalog instance.
// The catalog must be initialized via Initialize() before use.
func NewSystemCatalog(ps *memory.PageStore, tm *memory.TableManager) *SystemCatalog {
	sc := &SystemCatalog{
		store:        ps,
		tableManager: tm,
	}

	sc.loader = NewSchemaLoader(0, sc.iterateTable)
	return sc
}

// Initialize creates the system catalog tables (CATALOG_TABLES and CATALOG_COLUMNS)
// and registers them with the table manager. This must be called before any other
// catalog operations.
func (sc *SystemCatalog) Initialize(ctx *transaction.TransactionContext, dataDir string) error {
	defer sc.store.CommitTransaction(ctx)

	var err error
	sc.tablesTableID, err = sc.createCatalogTable(dataDir, CatalogTableFileName, CatalogTable, "table_id", GetTablesSchema())
	if err != nil {
		return fmt.Errorf("failed to create tables catalog: %w", err)
	}

	sc.columnsTableID, err = sc.createCatalogTable(dataDir, CatalogColumnsFileName, ColumnsTable, "", GetColumnsSchema())
	if err != nil {
		return fmt.Errorf("failed to create columns catalog: %w", err)
	}

	sc.loader.columnsTableID = sc.columnsTableID
	return nil
}

// createCatalogTable creates a heap file for a system catalog table and registers it
// with the table manager. This is an internal helper for Initialize().
func (sc *SystemCatalog) createCatalogTable(dataDir, fileName, tableName, primaryKey string, schema *tuple.TupleDescription) (int, error) {
	f, err := heap.NewHeapFile(
		fmt.Sprintf("%s/%s", dataDir, fileName),
		schema,
	)
	if err != nil {
		return 0, err
	}

	if err := sc.tableManager.AddTable(f, tableName, primaryKey); err != nil {
		return 0, err
	}

	return f.GetID(), nil
}

// RegisterTable adds a new table to the system catalog.
// It inserts metadata into CATALOG_TABLES and column definitions into CATALOG_COLUMNS.
func (sc *SystemCatalog) RegisterTable(tx *transaction.TransactionContext, tableID int, tableName, filePath, primaryKey string, fields []FieldMetadata,
) error {
	tup := createTablesTuple(tableID, tableName, filePath, primaryKey)
	if err := sc.store.InsertTuple(tx, sc.tablesTableID, tup); err != nil {
		return fmt.Errorf("failed to insert table metadata: %w", err)
	}

	for pos, f := range fields {
		tup = createColumnsTuple(tableID, f.Name, f.Type, pos, f.Name == primaryKey)
		if err := sc.store.InsertTuple(tx, sc.columnsTableID, tup); err != nil {
			return fmt.Errorf("failed to insert column metadata: %w", err)
		}
	}

	return nil
}

// createTablesTuple creates a tuple for insertion into CATALOG_TABLES.
// Schema: (table_id INT, table_name STRING, file_path STRING, primary_key STRING)
func createTablesTuple(tableID int, tableName, filePath, primaryKey string) *tuple.Tuple {
	t := tuple.NewTuple(GetTablesSchema())
	t.SetField(0, types.NewIntField(int32(tableID)))
	t.SetField(1, types.NewStringField(tableName, types.StringMaxSize))
	t.SetField(2, types.NewStringField(filePath, types.StringMaxSize))
	t.SetField(3, types.NewStringField(primaryKey, types.StringMaxSize))
	return t
}

// createColumnsTuple creates a tuple for insertion into CATALOG_COLUMNS.
// Schema: (table_id INT, column_name STRING, column_type INT, position INT, is_primary BOOL)
func createColumnsTuple(tableID int, colName string, colType types.Type, position int, isPrimary bool) *tuple.Tuple {
	t := tuple.NewTuple(GetColumnsSchema())
	t.SetField(0, types.NewIntField(int32(tableID)))
	t.SetField(1, types.NewStringField(colName, types.StringMaxSize))
	t.SetField(2, types.NewIntField(int32(colType)))
	t.SetField(3, types.NewIntField(int32(position)))
	t.SetField(4, types.NewBoolField(isPrimary))
	return t
}

type FieldMetadata struct {
	Name string
	Type types.Type
}

// LoadTables reads all table metadata from CATALOG_TABLES, reconstructs their schemas
// from CATALOG_COLUMNS, opens their heap files, and registers them with the table manager.
//
// This is called during database startup to restore all user tables.
func (sc *SystemCatalog) LoadTables(tx *transaction.TransactionContext, dataDir string) error {
	defer sc.store.CommitTransaction(tx)

	return sc.iterateTable(sc.tablesTableID, tx.ID, func(tableTuple *tuple.Tuple) error {
		tableID, name, filePath := parseTableMetadata(tableTuple)

		schema, pk, err := sc.loader.LoadTableSchema(tx.ID, tableID)
		if err != nil {
			return fmt.Errorf("failed to load schema for table %s: %w", name, err)
		}

		f, err := heap.NewHeapFile(filePath, schema)
		if err != nil {
			return fmt.Errorf("failed to open heap file for %s: %w", name, err)
		}

		if err := sc.tableManager.AddTable(f, name, pk); err != nil {
			return fmt.Errorf("failed to add table %s: %w", name, err)
		}

		return nil
	})
}

// GetTableID looks up the heap file ID for a table by name.
// Returns -1 and an error if the table is not found in the catalog.
func (sc *SystemCatalog) GetTableID(tid *primitives.TransactionID, tableName string) (int, error) {
	var result int = -1

	err := sc.iterateTable(sc.tablesTableID, tid, func(tableTuple *tuple.Tuple) error {
		tableID, name, _ := parseTableMetadata(tableTuple)

		if name == tableName {
			result = tableID
			return fmt.Errorf("found") // Use error to break iteration
		}
		return nil
	})

	if err != nil && err.Error() == "found" {
		return result, nil
	}
	if err != nil {
		return -1, err
	}

	return -1, fmt.Errorf("table %s not found in catalog", tableName)
}

// iterateTable scans all tuples in a table and applies a processing function to each.
// This is used internally for catalog queries like LoadTables() and GetTableID().
//
// The processFunc can return an error to stop iteration early.
func (sc *SystemCatalog) iterateTable(tableID int, tid *primitives.TransactionID, processFunc func(*tuple.Tuple) error) error {
	file, err := sc.tableManager.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file: %w", err)
	}

	iter := file.Iterator(tid)
	if err := iter.Open(); err != nil {
		return fmt.Errorf("failed to open iter: %w", err)
	}
	defer iter.Close()

	for {
		hasNext, err := iter.HasNext()
		if err != nil || !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil || tup == nil {
			break
		}

		if err := processFunc(tup); err != nil {
			return err
		}
	}

	return nil
}

// parseTableMetadata extracts table metadata fields from a CATALOG_TABLES tuple
func parseTableMetadata(tableTuple *tuple.Tuple) (tableID int, tableName, filePath string) {
	return getIntField(tableTuple, 0),
		getStringField(tableTuple, 1),
		getStringField(tableTuple, 2)
}
