package lock

import (
	"storemy/pkg/primitives"
	"time"
)

type LockType int

const (
	SharedLock LockType = iota
	ExclusiveLock
)

type Lock struct {
	TID       *primitives.TransactionID
	LockType  LockType
	GrantTime time.Time
}

type LockRequest struct {
	TID      *primitives.TransactionID
	LockType LockType
	Chan     chan bool // Channel to signal when lock is granted
}

func NewLock(tid *primitives.TransactionID, lockType LockType) *Lock {
	return &Lock{
		TID:       tid,
		LockType:  lockType,
		GrantTime: time.Now(),
	}
}

func NewLockRequest(tid *primitives.TransactionID, lockType LockType) *LockRequest {
	return &LockRequest{
		TID:      tid,
		LockType: lockType,
		Chan:     make(chan bool, 1),
	}
}
