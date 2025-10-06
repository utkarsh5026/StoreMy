package context

import (
	"storemy/pkg/catalog"
	"storemy/pkg/log"
	"storemy/pkg/memory"
)

// DatabaseContext holds all shared components that are needed across the database system.
// This provides a single source of truth and avoids passing multiple dependencies everywhere.
type DatabaseContext struct {
	tableManager *memory.TableManager
	pageStore    *memory.PageStore
	catalog      *catalog.SystemCatalog
	wal          *log.WAL
	dataDir      string
}

// NewDatabaseContext creates a new database context with all required components
func NewDatabaseContext(
	tableManager *memory.TableManager,
	pageStore *memory.PageStore,
	catalog *catalog.SystemCatalog,
	wal *log.WAL,
	dataDir string,
) *DatabaseContext {
	return &DatabaseContext{
		tableManager: tableManager,
		pageStore:    pageStore,
		catalog:      catalog,
		wal:          wal,
		dataDir:      dataDir,
	}
}

func (ctx *DatabaseContext) TableManager() *memory.TableManager {
	return ctx.tableManager
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
