package data

import (
	"LingDB/LingDB-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
)

// DataFile 数据文件，抽象存放数据的文件
type DataFile struct {
	FileId    uint32        //文件id
	WriteOff  int64         //文件写到哪了，偏移量
	IoManager fio.IOManager //io操作接口
}

// OpenDataFile 打开新的数据文件
// 根据文件的配置路径以及文件id就可以拼装文件的url了
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	//初始化IOManager管理器接口
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId)
}

// OpenHintFile 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

// OpenMergeFinishedFile 打开标识 merge 完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	//生成文件名称，文件的名是9位的数字
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
	//初始化IOManager管理器接口
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord 传入文件偏移量，返回解析后的记录、这条记录的长度、err
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	//获取文件最大长度，用于判断是否读溢出
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		//如果当前按照最大头部长度读取超出了文件的最大长度，那么不按照最大头部长度来读取，按照所剩长度读取
		headerBytes = fileSize - offset
	}

	//读取header信息，这里按照最大头部长度都读进来，可能会超读
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	//对读取到的头部信息进行解码操作
	header, headerSize := decodeLogRecordHeader(headerBuf)
	//下面两个条件都表示读取到了文件末尾，直接返回EOF错误
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{
		Type: header.recordType,
	}
	//读取实际的key和value值
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		//获取key和value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	//校验crc是否正确
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}

	df.WriteOff += int64(n)
	return nil
}

// WriteHintRecord 写入索引信息到 hint 文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

// Sync 持久化到硬盘
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// 读取N个字节
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}
