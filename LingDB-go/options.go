package LingDB_go

type Options struct {
	DirPath      string //数据库的数据存储目录
	DataFileSize int64  //数据文件的大小限制
	SyncWrites   bool   //每次写数据是否持久化
}
