package index

import (
	"LingDB/LingDB-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
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

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTree 索引迭代器
type btreeIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key+位置索引信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	//生成快照数组
	values := make([]*Item, tree.Len())

	//将所有的数据存放到数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	//判断正序逆序
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

func (bti *btreeIterator) Close() {
	bti.values = nil
}
