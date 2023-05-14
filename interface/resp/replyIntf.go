package resp

// Reply Reply信息都能转换为Byte切片
type ReplyIntf interface {
	ToBytes() []byte
}
