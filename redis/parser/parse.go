package parser

import (
	"bufio"
	"errors"
	"goRedis/interface/redis"
	"io"
)

/* 把客服端发来的消息进行解析*/

// Payload stores redis.Reply or error
type Payload struct {
	Data redis.Reply // 客户端发送的消息和服务端回复的是一样的，所以公用reply
	Err  error
}

// readState stores parser status
type readState struct {
	readingMultiLine  bool     //parser one or multi data
	expectedArgsCount int      // 期望的参数个数
	msgType           byte     // 信息类型
	args              [][]byte // 传过来的数据
	bulkLen           int64    // 字符串长度
}

// 判断是否执行完成
func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// ParseStream reads data form io.Reader and send payloads through channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch) // 为了异步的做协议的解析
	return ch
}

// parse0 协议解析
func parse0(reader io.Reader, ch chan<- *Payload) {

}

// readLine read one line data
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	if state.bulkLen == 0 { // 1、\r\n 直接切分
		msg, err = bufReader.ReadBytes('\n') // 这里我们按照\n切分
		if err != nil {
			return nil, true, err
		}
		if len(msg) <= 2 || msg[len(msg)-2] != '\r' { // 前面必须是\r 如果长度小于等于2 直接错误
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else { // 2、之前读到了$数字，严格读取字符个数
		msg = make([]byte, state.bulkLen+2)   // +2的意思是把\r\n 也读进来
		_, err := io.ReadFull(bufReader, msg) // 将bufReader 塞满 msg的空间
		if err != nil {
			return nil, true, err
		}
		if len(msg) <= 2 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	//var expectedLine uint64
	//expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	//if err != nil {
	//	return errors.New("protocol error: " + string(msg))
	//}
	//if expectedLine == 0 {
	//	state.expectedArgsCount = 0
	//} else if expectedLine > 0 {
	//	state.expectedArgsCount = int(expectedLine)
	//}
	return err
}
