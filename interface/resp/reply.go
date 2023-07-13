package resp

// Reply is the interface of resp serialization reply message
type Reply interface {
	ToBytes() []byte
}
