package protocol

var unknownErrBytes = []byte("-Err unknown\r\n")

type UnKnownErrReply struct {
}

func (r *UnKnownErrReply) Error() string {
	return "Err unknown"
}

func (r *UnKnownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

func MakeUnknownErrReply() *UnKnownErrReply {
	return &UnKnownErrReply{}
}

type ArgNumErrReply struct {
	CmdName string
}

func (r *ArgNumErrReply) Error() string {
	return "Err wrong number of arguments for '" + r.CmdName + "' command"
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("Err wrong number of arguments for '" + r.CmdName + "' command\r\n")
}

func MakeArgNumErrReply(cmdName string) *ArgNumErrReply {
	return &ArgNumErrReply{
		CmdName: cmdName,
	}
}

var SyntaxErrBytes = []byte("-Err syntax error\r\n")

type SyntaxErrReply struct {
}

func (r *SyntaxErrReply) Error() string {
	return "Err syntax error"
}

func (r *SyntaxErrReply) ToBytes() []byte {
	return SyntaxErrBytes
}

func MakeSyntaxErrReply() *SyntaxErrReply {
	return &SyntaxErrReply{}
}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

type WrongTypeErrReply struct {
}

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}
