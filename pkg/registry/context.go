package registry

import (
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/memory"
	"storemy/pkg/memory/wrappers/table"
)

// DatabaseContext holds all shared components that are needed across the database system.
// This provides a single source of truth and avoids passing multiple dependencies everywhere.
type DatabaseContext struct {
	pageStore    *memory.PageStore
	catalogMgr   *catalogmanager.CatalogManager
	txRegistry   *transaction.TransactionRegistry
	tupleManager *table.TupleManager
	wal          *log.WAL
	dataDir      string
}

// NewDatabaseContext creates a new database context with all required components
func NewDatabaseContext(
	pageStore *memory.PageStore,
	catalogMgr *catalogmanager.CatalogManager,
	wal *log.WAL,
	dataDir string,
) *DatabaseContext {
	tupleManager := table.NewTupleManager(pageStore)
	return &DatabaseContext{
		pageStore:    pageStore,
		catalogMgr:   catalogMgr,
		txRegistry:   transaction.NewTransactionRegistry(wal),
		tupleManager: tupleManager,
		wal:          wal,
		dataDir:      dataDir,
	}
}

func (ctx *DatabaseContext) PageStore() *memory.PageStore {
	return ctx.pageStore
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

func (ctx *DatabaseContext) CatalogManager() *catalogmanager.CatalogManager {
	return ctx.catalogMgr
}

func (ctx *DatabaseContext) TupleManager() *table.TupleManager {
	return ctx.tupleManager
}
