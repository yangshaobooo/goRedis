package resp

// Connection represents a connection with resp client
type Connection interface {
	Write([]byte) error
	GetDBIndex() int
	SelectDB(int)
}
