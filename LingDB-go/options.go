package LingDB_go

type Options struct {
	DirPath      string      //数据库的数据存储目录
	DataFileSize int64       //数据文件的大小限制
	SyncWrites   bool        //每次写数据是否持久化
	IndexType    IndexerType //数据索引类型
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	Prefix  []byte // 遍历前缀为指定值的 Key，默认为空
	Reverse bool   // 是否反向遍历，默认 false 是正向
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	MaxBatchNum uint // 一个批次当中最大的数据量
	SyncWrites  bool // 提交时是否 sync 持久化
}

type IndexerType = int8

const (
	// BTREE B树索引
	BTREE IndexerType = iota + 1
	// ART Adaptive Radix Tree自适应基数树索引
	ART
)

var DefaultOptions = Options{
	DirPath:      "./db-data",
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BTREE,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
