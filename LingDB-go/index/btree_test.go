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
