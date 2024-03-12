package index

import (
	"LingDB/LingDB-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 抽象索引接口
type Indexer interface {
	//Put key：存储键，pos：存储位置
	Put(key []byte, pos *data.LogRecordPos) bool

	//Get 根据key来查找磁盘索引位置
	Get(key []byte) *data.LogRecordPos

	//Delete 根据key来删除节点
	Delete(key []byte) bool

	// Size 获取索引中的数据量
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// BTREE B树索引
	BTREE IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case BTREE:
		return NewBTree()
	case ART:
		// todo:自适应基数树索引待实现
		return nil
	default:
		panic("unsupported index type")
	}
}

// Item google btree的树节点接口，为了保证节点的插入有序性，需要用户自己去是实现Less方法
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 比较规则就按照key的比特顺序进行排序
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

type Iterator interface {
	// Rewind 重新回到迭代器起点，也就是第一个数据
	Rewind()

	// Seek 根据传入的key查找到第一个大于(或者小于)该key的目标key，根据这个key开始遍历
	Seek(key []byte)

	// Next 跳转到下一个key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的Value数据
	Key() []byte

	// Value 当前遍历位置的Val
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放响应资源
	Close()
}
