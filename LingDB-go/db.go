package LingDB_go

import (
	"LingDB/LingDB-go/data"
	"LingDB/LingDB-go/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask存储引擎实例，用户用来操作数据库的对象
type DB struct {
	options    Options                   //用户配置项
	mu         *sync.RWMutex             //操作db需要加锁
	fileIds    []int                     //文件的id，只能用在加载索引时使用，不能修改这个属性的值和内部指针
	activeFile *data.DataFile            //当前活跃数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile //旧的数据文件，只能用于读
	index      index.Indexer             //内存索引
	seqNo      uint64                    // 事务序列号，全局递增
	isMerging  bool                      // 是否正在 merge
}

// Open 打开db存储引擎实例
func Open(options Options) (*DB, error) {
	//对用户传入配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	//校验目录是否存在，如果不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化DB实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(index.IndexType(options.IndexType)),
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	//加载数据文件内容
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从 hint 索引文件中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	//从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close 关闭活跃文件和旧数据文件
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	//关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	//关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync 刷盘
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

// Put 写入KV数据，key不能为nil
func (db *DB) Put(key []byte, value []byte) error {
	//如果传递进来的key为nil，那么直接返回nil异常
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//根据kv构造一个记录对象LogRecord，记录对象表示落盘的一条记录
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//添加记录到文件
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	//文件写入后更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Delete 删除操作
// 如果内存中没有key，那么就不需要追加日志，如果有，那么就向磁盘中追加日志，然后删除内存中的key
func (db *DB) Delete(key []byte) error {
	//判断key的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//先检查key在索引里是否存在，如果不存在的话直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	//构造LogRecord记录对象，标记该记录是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	//将删除操作追加到数据文件中
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	//删除内存中对应的key
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get 获取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	//检验key
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//从内存数据结构中取出key对应的索引信息
	logRecordPos := db.index.Get(key)
	//如果key在内存索引中找不到，那么就说明key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPosition(logRecordPos)
}

// ListKeys 获取所有的key，返回二位数组，key[i]的i是迭代器下表
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据，并执行用户指定的操作，函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// 根据指针获取硬盘文件中的数据
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	//根据文件的id找到对应的数据文件
	var dataFile *data.DataFile
	if logRecordPos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	//如果数据文件为nil
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//根据当前的文件id以及偏移量寻找文件数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	//如果磁盘中的数据类型是被删除，那么返回没找到
	//如果一个数据被删除，那么对于索引来说key会被更新为最小的value，而磁盘文件中该记录会被以日志的形式记录
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	//如果文件存在，能找到这个value且type不是被删除，那么返回value
	return logRecord.Value, nil
}
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 添加记录方法，追加的形势
// 添加记录需要通过db对文件进行操作，所以只能串行化去写，需要加锁
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	//检测当前活跃文件是否存在，如果不存在，那么需要初始化活跃文件
	if db.activeFile == nil {
		//初始化文件，因为刚开始启动的时候没有初始文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//这里db对象就持有活跃文件对象了
	//对记录对象进行编码，编码为文件写入字节流
	encRecord, size := data.EncodeLogRecord(logRecord)

	//写入前的活跃文件检测
	//判断是否可能写满当前活跃文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//先持久化数据，保证已有的数据持久化到硬盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//持久化后需要将当前活跃文件转换为旧数据文件
		//将当前活跃文件放入到旧数据文件map集合中，id为key
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		//打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//执行数据写入操作
	//记录当前记录的开始位置，用作索引的value中的文件偏移量指针
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//这里写入了只是写入到了操作系统缓存区，并没有立即入盘，这里需要根据用户配置来判断是否立即刷盘
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//返回内存索引信息，一条记录如果想定位到磁盘，那么需要他的文件id，文件内偏移量
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

// 设置活跃文件：该方法需要在初始化/当前活跃文件写满的情况下调用
// 注意调用这种数据库DB实例的共享数据改变操作方法，必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		//如果当前活跃文件不是nil，那么新的活跃文件id是当前活跃文件id+1
		initialFileId = db.activeFile.FileId + 1
	}

	//打开新的数据文件(路径由用户配置)
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 加载数据文件，db的activeFile以及olderFiles
func (db *DB) loadDataFiles() error {
	//根据配置项读取目录
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	//文件id
	var fileIds []int
	//遍历目录中的所有文件，如果以.data结尾，那就是数据文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			//如果数据目录被外部篡改
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	//对文件id进行排序，因为我们写入文件是从0开始的
	//后续文件可能会删除前面文件的记录，所以必须按照顺序读取
	sort.Ints(fileIds)
	db.fileIds = fileIds
	//遍历文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		//如果是最后一个文件，那么就是活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else { //如果不是那都是旧文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引
// 遍历所有文件中的数据，并且更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	//如果是0，那么是空的数据库，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		// 因为按照文件id顺序遍历的，所以如果后续有追加了delete的record，那么需要删除这个索引中的kv
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	//遍历所有文件id，处理文件数据
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 如果比最近未参与 merge 的文件 id 更小，则说明已经从 Hint 文件中加载索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		//判断是否是活跃文件
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		//循环处理每一行Record
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				//如果是io问题，那么退出循环
				if err == io.EOF {
					break
				}
				return err
			}

			//构造内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}

			// 解析 key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对应的 seq no 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			//底座offset，下一次读取下一个Record
			offset += size
		}

		//该文件读取完毕
		//如果这个文件是当前活跃文件，那么需要记录当前文件写入指针（写入偏移量），方便put追加
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must to be greater than 0")
	}
	return nil
}
