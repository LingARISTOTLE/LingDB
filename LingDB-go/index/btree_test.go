package index

import (
	"LingDB/LingDB-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    2,
		Offset: 100,
	})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    2,
		Offset: 100,
	})
	assert.True(t, res2)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	t.Log(pos1)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(2), pos2.Fid)
	assert.Equal(t, int64(100), pos2.Offset)

	t.Log(pos2)
}

func TestBTree_Delete(t *testing.T) {
	bTree := NewBTree()

	b1 := bTree.Delete([]byte("a"))
	t.Log(b1)
	assert.False(t, b1)

	res1 := bTree.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.True(t, res1)

	b2 := bTree.Delete([]byte("a"))
	assert.True(t, b2)

}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	// 1.BTree 为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	//	2.BTree 有数据的情况
	bt1.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.有多条数据
	bt1.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 4.测试 seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 5.反向遍历的 seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
