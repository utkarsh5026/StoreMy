package registry

import (
	"storemy/pkg/catalog"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/memory"
)

// DatabaseContext holds all shared components that are needed across the database system.
// This provides a single source of truth and avoids passing multiple dependencies everywhere.
type DatabaseContext struct {
	pageStore  *memory.PageStore
	catalog    *catalog.SystemCatalog
	catalogMgr *catalog.CatalogManager
	txRegistry *transaction.TransactionRegistry
	wal        *log.WAL
	dataDir    string
}

// NewDatabaseContext creates a new database context with all required components
func NewDatabaseContext(
	pageStore *memory.PageStore,
	catalog *catalog.SystemCatalog,
	catalogMgr *catalog.CatalogManager,
	wal *log.WAL,
	dataDir string,
) *DatabaseContext {
	return &DatabaseContext{
		pageStore:  pageStore,
		catalog:    catalog,
		catalogMgr: catalogMgr,
		txRegistry: transaction.NewTransactionRegistry(wal),
		wal:        wal,
		dataDir:    dataDir,
	}
}

func (ctx *DatabaseContext) PageStore() *memory.PageStore {
	return ctx.pageStore
}

func (ctx *DatabaseContext) Catalog() *catalog.SystemCatalog {
	return ctx.catalog
}

func (ctx *DatabaseContext) WAL() *log.WAL {
	return ctx.wal
}

func (ctx *DatabaseContext) DataDir() string {
	return ctx.dataDir
}

func (ctx *DatabaseContext) TransactionRegistry() *transaction.TransactionRegistry {
	return ctx.txRegistry
}

func (ctx *DatabaseContext) CatalogManager() *catalog.CatalogManager {
	return ctx.catalogMgr
}
