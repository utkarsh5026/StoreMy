package planner

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
)

type DbIterator = iterator.DbIterator
type TID = *primitives.TransactionID
type DbContext = *registry.DatabaseContext
type TupleDesc = *tuple.TupleDescription
type TransactionCtx = *transaction.TransactionContext
