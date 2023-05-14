package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"memgo/interface/resp"
	protocol2 "memgo/redis/RESP/protocol"
	"strconv"
)

// PayLoad 有效载荷
type PayLoad struct {
	Data resp.ReplyIntf
	Err  error
}

// ParseStream 将解析命令和执行命令划分开，进行异步处理
func ParseStream(reader io.Reader) <-chan *PayLoad {
	ch := make(chan *PayLoad)
	go parse0(reader, ch)
	return ch
}

// TODO 健壮性感觉有问题，需要确认一下
func parse0(rawReader io.Reader, ch chan<- *PayLoad) {

	reader := bufio.NewReader(rawReader)
	for {
		line, err := readLineOrHeader(reader)
		if err != nil {
			ch <- &PayLoad{
				Err: err,
			}
			close(ch)
			return
		}
		line = bytes.TrimSuffix(line, []byte("\r\n"))
		// 解析了SingleLineReply 或 多行Reply 接下来看是哪种类型
		var result resp.ReplyIntf // 接收解析后的reply报文
		var parseErr error        // 返回解析错误
		// NODE 发生IO错误则终止解析, 相当于解析器停止运行
		var flag bool // 标识是否为io错误
		switch line[0] {
		case '+', '-', ':':
			result, parseErr, flag = parseSingleLineReply(line)
		case '$':
			result, parseErr, flag = parseBulkString(line, reader)
		case '*':
			result, parseErr, flag = parseMultiBulk(line, reader)
		// 除了 RESP 之外 redis 还有一个简单的 Text protocol. 按空格隔开是为了解析 text protocol
		default:
			args := bytes.Split(line, []byte{' '})
			result = protocol2.MakeMultiBulkReply(args)
			parseErr = nil
		}
		// 通过 channel 发送数据有效载荷
		ch <- &PayLoad{
			Data: result,
			Err:  parseErr,
		}
		// 若解析时发生IO错误，停止解析剩余命令
		if flag {
			close(ch)
			return
		}
	}
}

// readLine 为确保二进制安全，不能以 换行符CRLF 进行切分，应该按照 ReplyHeader的长度信息来读取
// readLine 返回的格式为 ...\r\n
func readLineOrHeader(bufReader *bufio.Reader) ([]byte, error) {
	// 通过 \r\n切分;
	var msg []byte
	var err error
	msg, err = bufReader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	// msg[len(msg)-1] = '\n' 因为这是以 '\n' 进行分割的，所以无需判断 \n
	if len(msg) == 0 || msg[len(msg)-2] != '\r' {
		return nil, errors.New("protocol error: " + string(msg))
	}

	return msg, nil
}

func parseMultiBulk(header []byte, reader *bufio.Reader) (resp.ReplyIntf, error, bool) {
	var expectedLine int64
	var err error
	expectedLine, err = strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || expectedLine < 0 {
		return nil, errors.New("protocol error--illegal number: " + string(header)), false
	}
	if expectedLine == 0 {
		return protocol2.MakeEmptyMultiBulkReply(), err, false
	}
	lines := make([][]byte, 0, expectedLine)
	for i := int64(0); i < expectedLine; i++ {
		var bulkHeader []byte
		bulkHeader, err = reader.ReadBytes('\n')
		// io错误检验
		if err != nil {
			return nil, err, true
		}
		// header合法性检验
		length := len(bulkHeader)
		if length < 4 || bulkHeader[length-2] != '\r' || bulkHeader[0] != '$' {
			return nil, errors.New("protocol error--illegal BulkString header: " + string(bulkHeader)), false
		}
		// 采用的错误处理策略是，多行命令中若有某行命令出错则全部丢弃
		var bulkLen int64
		bulkLen, err = strconv.ParseInt(string(bulkHeader[1:length-2]), 10, 64)
		if err != nil || bulkLen < -1 {
			return nil, errors.New("protocol error--illegal number: " + string(bulkHeader)), false
		} else if bulkLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, bulkLen+2)
			_, err = io.ReadFull(reader, body)
			// io错误
			if err != nil {
				return nil, err, true
			}
			lines = append(lines, body[:len(body)-2])
		}
	}
	return protocol2.MakeMultiBulkReply(lines), nil, false
}

func parseBulkString(header []byte, reader *bufio.Reader) (resp.ReplyIntf, error, bool) {
	var err error
	var bulkLen int64
	bulkLen, err = strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || bulkLen < -1 {
		return nil, errors.New("protocol error--illegal number: " + string(header)), false
	}
	if bulkLen == -1 {
		return protocol2.MakeNullBulkReply(), nil, false
	}
	// 空字符串也可以使用 $0\r\n\r\n 表示, 读进来的 header 为 $0 (已去除后缀) body 为 \r\n
	body := make([]byte, bulkLen+2)
	_, err = io.ReadFull(reader, body)
	// io错误
	if err != nil {
		return nil, err, true
	}
	return protocol2.MakeBulkReply(body[:len(body)-2]), nil, false
}

func parseSingleLineReply(msg []byte) (resp.ReplyIntf, error, bool) {
	var result resp.ReplyIntf
	switch msg[0] {
	case '+':
		result = protocol2.MakeStatusReply(string(msg[1:]))
	case '-':
		result = protocol2.MakeErrReply(string(msg[1:]))
	case ':':
		code, err := strconv.ParseInt(string(msg[1:]), 10, 64)
		if err != nil {
			return nil, errors.New("protocol error--illegal number: " + string(msg)), false
		}
		result = protocol2.MakeIntReply(code)
	}
	return result, nil, false
}
