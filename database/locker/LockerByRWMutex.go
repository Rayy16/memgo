package locker

import (
	"memgo/interface/locker"
	"sync"
)

type LockerByRWMutex struct {
	isLocked bool
	mu       sync.RWMutex
}

func (locker *LockerByRWMutex) IsDictLocked() bool {
	return locker.isLocked
}

func (locker *LockerByRWMutex) RWLocks(writeKeys []string, readKeys []string) {
	locker.mu.Lock()
	locker.isLocked = true
}

func (locker *LockerByRWMutex) RWUnLocks(writeKeys []string, readKeys []string) {
	locker.mu.Unlock()
	locker.isLocked = false
}

func MakeLockerByRWMutex() locker.LockerIntf {
	return &LockerByRWMutex{
		isLocked: false,
		mu:       sync.RWMutex{},
	}
}
