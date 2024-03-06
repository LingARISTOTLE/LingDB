package data

// LogRecordType 定义记录的枚举类型，因为记录有过期和非过期之说
type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord 写入到数据文件的记录
// 之所以叫日志记录，是因为bitcask写入的数据都是以追加的形式去写入的，类似于日志的实现
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType //定义这条记录的类型（过期，非过期等）
}

// LogRecordPos 内存索引数据结构主要描述数据在磁盘上的位置
// 一条记录如果想定位到磁盘，那么需要他的文件id，文件内偏移量
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示将数据存储到了那个文件中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的那个位置
}

// EncodeLogRecord 对LogRecord进行编码，返回字节数组及长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
