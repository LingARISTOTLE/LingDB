package fio

// DataFilePerm 文件打开权限默认值
const DataFilePerm = 0644

// IOManager io管理器
type IOManager interface {
	//Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	//Write 写入字节数组到文件中
	Write([]byte) (int, error)

	//Sync 持久化数据
	Sync() error

	//Close 关闭文件
	Close() error
}
