package logging

import (
	"log/slog"
)

// WithTx creates a logger with transaction context.
// Use this to automatically include transaction ID in all logs.
//
// Example:
//
//	log := logging.WithTx(tx)
//	log.Info("starting operation")
//	log.Debug("processing", "rows", count)
func WithTx(txID int) *slog.Logger {
	return GetLogger().With("tx_id", txID)
}

// WithTable creates a logger with table context.
// Use this for catalog and table operations.
//
// Example:
//
//	log := logging.WithTable("users")
//	log.Info("table operation", "action", "create")
func WithTable(tableName string) *slog.Logger {
	return GetLogger().With("table", tableName)
}

// WithTableTx creates a logger with both transaction and table context.
//
// Example:
//
//	log := logging.WithTableTx(tx, "orders")
//	log.Info("inserting rows", "count", 10)
func WithTableTx(txID int, tableName string) *slog.Logger {
	return GetLogger().With("tx_id", txID, "table", tableName)
}

// WithIndex creates a logger with index context.
//
// Example:
//
//	log := logging.WithIndex("idx_user_email")
//	log.Debug("index lookup", "key", email)
func WithIndex(indexName string) *slog.Logger {
	return GetLogger().With("index", indexName)
}

// WithPage creates a logger with page context.
// Useful for buffer pool and storage operations.
//
// Example:
//
//	log := logging.WithPage(pageID)
//	log.Debug("page pinned", "dirty", isDirty)
func WithPage(pageID int) *slog.Logger {
	return GetLogger().With("page_id", pageID)
}

// WithLock creates a logger with lock context.
// Useful for concurrency and lock manager operations.
//
// Example:
//
//	log := logging.WithLock(txID, resourceID)
//	log.Info("lock acquired", "lock_type", "exclusive")
func WithLock(txID int, resourceID string) *slog.Logger {
	return GetLogger().With("tx_id", txID, "resource", resourceID)
}

// WithComponent creates a logger with component/subsystem context.
//
// Example:
//
//	log := logging.WithComponent("catalog")
//	log.Info("component initialized")
func WithComponent(component string) *slog.Logger {
	return GetLogger().With("component", component)
}

// WithError creates a logger with error context.
// Use this when logging errors to include the error in structured format.
//
// Example:
//
//	log := logging.WithError(err)
//	log.Error("operation failed", "operation", "insert")
func WithError(err error) *slog.Logger {
	return GetLogger().With("error", err.Error())
}
