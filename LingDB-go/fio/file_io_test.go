package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("C:\\go-pj\\LingDB\\db_data", "0001.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("C:\\go-pj\\LingDB\\db_data", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, n, 0)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask test"))
	t.Log(n)
	assert.Equal(t, n, 12)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("storage"))
	t.Log(n)
	assert.Equal(t, n, 7)
	assert.Nil(t, err)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("C:\\go-pj\\LingDB\\db_data", "a.data")
	fio, err := NewFileIOManager(path)
	//从0开始读取到b中
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b)

	n, err = fio.Read(b, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("C:\\go-pj\\LingDB\\db_data", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("C:\\go-pj\\LingDB\\db_data", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
