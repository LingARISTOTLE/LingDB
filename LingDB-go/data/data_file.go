package data

import "LingDB/LingDB-go/fio"

const DataFileNameSuffix = ".data"

// DataFile 数据文件，抽象存放数据的文件
type DataFile struct {
	FileId    uint32        //文件id
	WriteOff  int64         //文件写到哪了，偏移量
	IoManager fio.IOManager //io操作接口
}

// OpenDataFile 打开新的数据文件
// 根据文件的配置路径以及文件id就可以拼装文件的url了
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// ReadLogRecord 传入文件偏移量，返回解析后的记录、这条记录的长度、err
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

// Sync 持久化到硬盘
func (df *DataFile) Sync() error {
	return nil
}
