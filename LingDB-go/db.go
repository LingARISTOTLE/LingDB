package LingDB_go

import (
	"LingDB/LingDB-go/data"
	"LingDB/LingDB-go/index"
	"sync"
)

// DB bitcask存储引擎实例，用户用来操作数据库的对象
type DB struct {
	options    Options                   //用户配置项
	mu         *sync.RWMutex             //操作db需要加锁
	activeFile *data.DataFile            //当前活跃数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile //旧的数据文件，只能用于读
	index      index.Indexer             //内存索引
}

// Put 写入KV数据，key不能为nil
func (db *DB) Put(key []byte, value []byte) error {
	//如果传递进来的key为nil，那么直接返回nil异常
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//根据kv构造一个记录对象LogRecord，记录对象表示落盘的一条记录
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//添加记录到文件
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	//文件写入后更新内存索引
	if ok := db.index.Put(key, pos); !ok {
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

	//根据当前的文件id以及偏移量寻找文件数据据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
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

// 添加记录方法，追加的形势
// 添加记录需要通过db对文件进行操作，所以只能串行化去写，需要加锁
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

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
