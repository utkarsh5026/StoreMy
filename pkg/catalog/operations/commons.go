package operations

import (
	"errors"
	"storemy/pkg/concurrency/transaction"
)

type (
	TxContext = *transaction.TransactionContext
)

var (
	ErrSuccess = errors.New("success")
)
