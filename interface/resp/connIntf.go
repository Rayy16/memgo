package resp

type ConnectionIntf interface {
	Write([]byte) (int, error)
	Close() error

	GetDBIndex() int
	SelectDB(int)
}
