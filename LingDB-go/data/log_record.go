package data

// LogRecordPos 内存索引数据结构主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示将数据存储到了那个文件中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的那个位置
}
