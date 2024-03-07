package fio

// DataFilePerm 文件打开权限默认值
const DataFilePerm = 0644

// IOManager io管理器，抽象的io接口，可以接入不同的IO类型，目前支持标准文件IO
type IOManager interface {
	//Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	//Write 写入字节数组到文件中
	Write([]byte) (int, error)

	//Sync 持久化数据
	Sync() error

	//Close 关闭文件
	Close() error

	// Size 获取文件大小
	Size() (int64, error)
}

// NewIOManager 初始化IOManager，目前只支持FileIO
func NewIOManager(fileName string) (IOManager, error) {
	//目前方法直接写死获取文件id，未来拓展io渠道时会新增枚举->对应io管理器
	return NewFileIOManager(fileName)
}
