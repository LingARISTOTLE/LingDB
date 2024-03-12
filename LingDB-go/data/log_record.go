package data

import (
	"encoding/binary"
	"hash/crc32"
)

// LogRecordType 定义记录的枚举类型，因为记录有过期和非过期之说
type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// crc type keySize valueSize
// 4 + 1 + 5 + 5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录
// 之所以叫日志记录，是因为bitcask写入的数据都是以追加的形式去写入的，类似于日志的实现
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType //定义这条记录的类型（过期，非过期等）
}

// LogRecord的头部信息
type logRecordHeader struct {
	crc        uint32        // crc校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// LogRecordPos 内存索引数据结构主要描述数据在磁盘上的位置
// 一条记录如果想定位到磁盘，那么需要他的文件id，文件内偏移量
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示将数据存储到了那个文件中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的那个位置
}

// TransactionRecord 暂存的事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对LogRecord进行编码，返回字节数组及长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	//初始化一个header的数组，按照最大头部长度来初始化
	header := make([]byte, maxLogRecordHeaderSize)

	//header的第5字节表示记录的类型，这里4是从0开始
	header[4] = record.Type

	//后面的keySize和ValueSize使用变长字符串从索引5开始操作
	var index = 5

	//这里使用binary的PutVarint方法操作，方法返回操作的长度
	//这个方法在写入数字时采用变长编码的方式，比如一个字节数组占用8位，那么那么会被切分为前7位和后一位，如果没有结束，那么最高位为1，如果结束了那么最高位为0
	//比如：1000111010
	//第一部分：1 1000111
	//第二部分：0 010
	index += binary.PutVarint(header[index:], int64(len(record.Key)))
	index += binary.PutVarint(header[index:], int64(len(record.Value)))

	//这里最终的长度大小已经确定，头部index + len(key) + len(value)
	//计算最终的写入数组长度
	var size = index + len(record.Key) + len(record.Value)
	//这时已经放入了头部除了crc的所有信息，那么将header从0到index内容拷贝到最终写入数组
	encBytes := make([]byte, size)

	//将header内容拷贝过来
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], record.Key)
	copy(encBytes[index+len(record.Key):], record.Value)

	//计算校验位
	crc := crc32.ChecksumIEEE(encBytes[4:])
	//以小端的方式写入，小端：低位在低地址，大端：低位在高地址
	//一个十六进制数 0x12345678 在大端存储下的内存顺序为 12 34 56 78 ==> 这里计算机读取都是从文件低位开始读的(从左向右)，那么处理数据时会先读取到高位，不方便操作
	//一个十六进制数 0x12345678 在小端存储中的内存顺序为 78 56 34 12 ==> 计算机读取时候会先读到低位
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	return encBytes, int64(size)
}

// 解码，传入字节数据，返回解码后的Header对象以及解码数组的长度
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	//获取实际的key和value
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// 获取crc，类似于计算机网络里的摘要算法
// 也就是说对数据部分进行摘要计算，生成一个摘要值，如果内容被篡改，那么头部的crc和根据内容计算出的crc会不一样
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
