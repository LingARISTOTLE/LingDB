package data

import "LingDB/LingDB-go/fio"

// DataFile 数据文件，抽象存放数据的文件
type DataFile struct {
	FileId    uint32        //文件id
	WriteOff  int64         //文件写到哪了，偏移量
	IoManager fio.IOManager //io操作接口
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

// Sync 持久化到硬盘
func (df *DataFile) Sync() error {
	return nil
}
