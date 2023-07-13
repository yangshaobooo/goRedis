package reply

/*
用于实现一些固定的redis回复
*/

// -----1、回复pong-----

type PongReply struct{}

var pongBytes = []byte("+PONG\r\n") // 正常回复用+开头

// ToBytes marshal resp reply
func (p *PongReply) ToBytes() []byte {
	return pongBytes
}
func MakePongReply() *PongReply {
	return &PongReply{}
}

// -----2、回复ok-----

type OkReply struct{}

var okBytes = []byte("+OK\r\n")

// ToBytes marshal resp reply
func (r *OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply) //在这里弄一个对象，省的每次make的时候弄一个新的，节省内存
func MakeOkReply() *OkReply {
	return theOkReply
}

// -----3、回复空的字符串-----

type NullBulkReply struct{}

var nullBulkBytes = []byte("$-1\r\n")

// ToBytes marshal resp reply
func (n *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}
func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// -----4、回复空的数组-----

type EmptyMultiBulkReply struct{}

var emptyMultiBulkReply = []byte("*0\r\n")

// ToBytes marshal resp reply
func (e *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkReply
}
func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

// -----5、回复空response nothing-----

type NoReply struct{}

var noBytes = []byte("")

// ToBytes marshal resp reply
func (n NoReply) ToBytes() []byte {
	return noBytes
}
