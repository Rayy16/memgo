package protocol

type PongReply struct {
}

var pongBytes = []byte("+PONG" + CRLF)

func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

func MakePongReply() *PongReply {
	return &PongReply{}
}

var okBytes = []byte("+OK" + CRLF)

type OkReply struct {
}

func (r *OkReply) ToBytes() []byte {
	return okBytes
}

func MakeOkReply() *OkReply {
	return &OkReply{}
}

var nullBulkBytes = []byte("$-1" + CRLF)

type NullBulkReply struct {
}

func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

var EmptyMultiBulkBytes = []byte("*0" + CRLF)

type EmptyMultiBulkReply struct {
}

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return EmptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

var noBytes = []byte("")

type NoReply struct {
}

func (r *NoReply) ToBytes() []byte {
	return noBytes
}
