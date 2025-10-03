package log

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/page"
)

type LogFile interface {
	LogTransactionBegin(tid *transaction.TransactionID) error
	LogCommit(tid *transaction.TransactionID) error
	LogAbort(tid *transaction.TransactionID) error
	LogUpdate(tid *transaction.TransactionID, before, after page.Page) error
	Close() error
}
