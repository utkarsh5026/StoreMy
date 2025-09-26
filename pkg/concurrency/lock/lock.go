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
