package lock

import (
	"storemy/pkg/concurrency/transaction"
	"time"
)

type LockType int

const (
	SharedLock LockType = iota
	ExclusiveLock
)

type Lock struct {
	TID       *transaction.TransactionID
	LockType  LockType
	GrantTime time.Time
}

type LockRequest struct {
	TID      *transaction.TransactionID
	LockType LockType
	Chan     chan bool // Channel to signal when lock is granted
}

func NewLock(tid *transaction.TransactionID, lockType LockType) *Lock {
	return &Lock{
		TID:       tid,
		LockType:  lockType,
		GrantTime: time.Now(),
	}
}

func NewLockRequest(tid *transaction.TransactionID, lockType LockType) *LockRequest {
	return &LockRequest{
		TID:      tid,
		LockType: lockType,
		Chan:     make(chan bool, 1),
	}
}
