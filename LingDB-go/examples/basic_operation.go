package main

import (
	lingDB "LingDB/LingDB-go"
	"fmt"
)

func main() {
	opts := lingDB.DefaultOptions
	db, err := lingDB.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("test_key"), []byte("test_value"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("test_key"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("test_key"))
	if err != nil {
		panic(err)
	}

	val, err = db.Get([]byte("test_key"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))
}
