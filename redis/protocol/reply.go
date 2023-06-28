package protocol

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}
