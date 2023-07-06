package redis

// Connection represents a connection with redis client
type Connection interface {
	Write([]byte) error
	GetDBIndex() int
	SelectDB(int)
}
