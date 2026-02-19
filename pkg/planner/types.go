package planner

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/planner/internal/shared"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
)

type DbIterator = iterator.DbIterator
type TID = *primitives.TransactionID
type DbContext = *registry.DatabaseContext
type TupleDesc = *tuple.TupleDescription
type TxContext = *transaction.TransactionContext
type TransactionCtx = *transaction.TransactionContext // Alias for backward compatibility

// Result types exported from internal/shared package
type SelectQueryResult = shared.SelectQueryResult
type DMLResult = shared.DMLResult
type DDLResult = shared.DDLResult
type ExplainResult = shared.ExplainResult
