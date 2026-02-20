package registry

import (
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/indexmanager"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	"storemy/pkg/memory/wrappers/table"
	"storemy/pkg/primitives"
)

// DatabaseContext holds all shared components that are needed across the database system.
// This provides a single source of truth and avoids passing multiple dependencies everywhere.
type DatabaseContext struct {
	pageStore    *memory.PageStore
	catalogMgr   *catalogmanager.CatalogManager
	indexManager *indexmanager.IndexManager
	txRegistry   *transaction.TransactionRegistry
	tupleManager *table.TupleManager
	wal          *wal.WAL
	dataDir      string
}

// catalogAdapter adapts catalogmanager.CatalogManager to indexmanager.CatalogReader
type catalogAdapter struct {
	cm *catalogmanager.CatalogManager
}

func (ca *catalogAdapter) GetIndexesByTable(tx *transaction.TransactionContext, tableID primitives.FileID) ([]*systable.IndexMetadata, error) {
	return ca.cm.GetIndexesByTable(tx, tableID)
}

func (ca *catalogAdapter) GetTableSchema(tableID primitives.FileID) (*schema.Schema, error) {
	return ca.cm.GetTableSchema(nil, tableID)
}

// NewDatabaseContext creates a new database context with all required components
func NewDatabaseContext(
	pageStore *memory.PageStore,
	catalogMgr *catalogmanager.CatalogManager,
	wal *wal.WAL,
	dataDir string,
) *DatabaseContext {
	tupleManager := table.NewTupleManager(pageStore)
	adapter := &catalogAdapter{cm: catalogMgr}
	indexMgr := indexmanager.NewIndexManager(adapter, pageStore, wal)

	tupleManager.SetIndexMaintainer(indexMgr)

	return &DatabaseContext{
		pageStore:    pageStore,
		catalogMgr:   catalogMgr,
		indexManager: indexMgr,
		txRegistry:   transaction.NewTransactionRegistry(wal),
		tupleManager: tupleManager,
		wal:          wal,
		dataDir:      dataDir,
	}
}

func (ctx *DatabaseContext) PageStore() *memory.PageStore {
	return ctx.pageStore
}

func (ctx *DatabaseContext) WAL() *wal.WAL {
	return ctx.wal
}

func (ctx *DatabaseContext) DataDir() string {
	return ctx.dataDir
}

func (ctx *DatabaseContext) TransactionRegistry() *transaction.TransactionRegistry {
	return ctx.txRegistry
}

func (ctx *DatabaseContext) CatalogManager() *catalogmanager.CatalogManager {
	return ctx.catalogMgr
}

func (ctx *DatabaseContext) TupleManager() *table.TupleManager {
	return ctx.tupleManager
}

func (ctx *DatabaseContext) IndexManager() *indexmanager.IndexManager {
	return ctx.indexManager
}
