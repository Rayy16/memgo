package locker

type LockerIntf interface {
	RWLocks(writeKeys []string, readKeys []string)
	RWUnLocks(writeKeys []string, readKeys []string)
}
