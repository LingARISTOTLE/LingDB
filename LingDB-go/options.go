package LingDB_go

type Options struct {
	DirPath      string      //数据库的数据存储目录
	DataFileSize int64       //数据文件的大小限制
	SyncWrites   bool        //每次写数据是否持久化
	IndexType    IndexerType //数据索引类型
}

type IndexerType = int8

const (
	// BTREE B树索引
	BTREE IndexerType = iota + 1
	// ART Adaptive Radix Tree自适应基数树索引
	ART
)
