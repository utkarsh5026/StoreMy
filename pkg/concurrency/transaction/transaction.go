package transaction

import (
	"fmt"
	"sync/atomic"
)

var transactionCounter int64

type TransactionID struct {
	id int64
}

func NewTransactionID() *TransactionID {
	return &TransactionID{
		id: atomic.AddInt64(&transactionCounter, 1),
	}
}

// NewTransactionIDFromValue creates a TransactionID with a specific ID value.
// This is primarily used for deserialization purposes.
func NewTransactionIDFromValue(id int64) *TransactionID {
	return &TransactionID{
		id: id,
	}
}

func (tid *TransactionID) ID() int64 {
	return tid.id
}

func (tid *TransactionID) String() string {
	return fmt.Sprintf("TID-%d", tid.id)
}

func (tid *TransactionID) Equals(other *TransactionID) bool {
	if tid == nil || other == nil {
		return tid == other
	}
	return tid.id == other.id
}
