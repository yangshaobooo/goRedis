package reply

// -----1、回复未知错误-----

type UnknownErrReply struct{}

var unknownErrBytes = []byte("-Err unknown\r\n")

func (u *UnknownErrReply) Error() string {
	return "Err unknown"
}

func (u *UnknownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

// -----2、ArgNumErrReply represent wrong number of arguments for command-----

type ArgNumErrReply struct {
	Cmd string
}

func (a *ArgNumErrReply) Error() string {
	return "-ERR wrong number of arguments for '" + a.Cmd + "' command"
}

func (a *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + a.Cmd + "' command\r\n")
}

func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

// -----3、语法错误 syntax error-----

type SyntaxErrReply struct{}

var syntaxErrReply = []byte("-Err syntax error\r\n")
var theSyntaxErrReply = &SyntaxErrReply{} // 这种固定的，用同一个，节省内存
func (s *SyntaxErrReply) Error() string {
	return "Err syntax error"
}

func (s *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrReply
}
func MakeSyntaxErrReply() *SyntaxErrReply {
	return theSyntaxErrReply
}

// -----4、数据类型错误-----

// WrongTypeErrReply represents operation against a key holding the wrong kind of value
type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

// ToBytes marshals resp.Reply
func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

// -----5、解析协议错误-----

// ProtocolErrReply represents meeting unexpected byte during parse requests
type ProtocolErrReply struct {
	Msg string
}

// ToBytes marshals resp.Reply
func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR Protocol error: '" + r.Msg + "'\r\n")
}

func (r *ProtocolErrReply) Error() string {
	return "ERR Protocol error '" + r.Msg + "' command"
}
