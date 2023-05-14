package parser

import (
	"bytes"
	"io"
	"memgo/interface/resp"
	protocol2 "memgo/redis/RESP/protocol"
	"testing"
)

func TestParseStream(t *testing.T) {
	replies := []resp.ReplyIntf{
		protocol2.MakeIntReply(1),
		protocol2.MakeStatusReply("OK"),
		protocol2.MakeErrReply("ERR unknown"),
		protocol2.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		protocol2.MakeMultiBulkReply([][]byte{
			[]byte("z"),
			[]byte("\r\n"),
		}),
		protocol2.MakeEmptyMultiBulkReply(),
	}
	reqs := bytes.Buffer{}
	for _, re := range replies {
		reqs.Write(re.ToBytes())
	}
	reqs.Write([]byte("set a a" + protocol2.CRLF)) // test text protocol
	expected := make([]resp.ReplyIntf, len(replies))
	copy(expected, replies)
	expected = append(expected, protocol2.MakeMultiBulkReply([][]byte{
		[]byte("set"), []byte("a"), []byte("a"),
	}))

	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expected[i]
		i++
		if !BytesEquals(exp.ToBytes(), payload.Data.ToBytes()) {
			t.Error("parse failed: " + string(exp.ToBytes()))
		}
	}
}

func BytesEquals(a []byte, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}
