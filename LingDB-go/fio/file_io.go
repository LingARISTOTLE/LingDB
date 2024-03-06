package fio

import "os"

// FileIO 标准系统文件，IO；这里是io_manager的一种实现，其他还有如MMap等多种实现
type FileIO struct {
	fd *os.File //系统文件描述符
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	file, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: file}, nil
}

// Read 从文件的给定位置读取对应的数据z
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Write 写入字节数组到文件中
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Sync 持久化数据
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
