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