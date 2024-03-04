package index

import (
	"LingDB/LingDB-go/data"
	"github.com/google/btree"
	"sync"
)

// BTree 索引，主要分装了 google 的 btree 库
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	//google的btree的写操作线程不安全，读操作线程安全，因此加锁是必要的
	lock *sync.RWMutex
}

// NewBTree 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		//叶子结点数量
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := &Item{key: key, pos: pos}
	//存储数据前需要加锁
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(item)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	item := &Item{key: key}
	btreeItem := bt.tree.Get(item)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	item := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(item)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}
