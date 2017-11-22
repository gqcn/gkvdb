// 基于DRH(Deep-Re-Hash)算法的高性能Key-Value嵌入式数据库

// 数据结构要点   ：数据的分配长度cap >= 数据真实长度len，且 cap - len <= bucket，
//               当数据存储内容发生改变时，依靠碎片管理器对碎片进行回收再利用，且碎片大小 >= bucket

// 索引文件结构  ：元数据文件偏移量倍数(36bit,64GB*元数据桶大小)|下一层级索引的文件偏移量倍数(重复分区标志位=1时有效) 元数据文件列表项大小(19bit,524287)|分区增量 深度分区标识符(1bit)
// 元数据文件结构 :[键名哈希64(64bit) 键名长度(8bit) 键值长度(24bit,16MB) 数据文件偏移量(40bit,1TB)](变长,链表)
// 数据文件结构  ：键名长度(8bit) 键名(变长) 键值(变长)


package gkvdb

import (
    "os"
    "g/os/gfile"
    "strings"
    "g/encoding/gbinary"
    "g/os/gfilepool"
    "errors"
    "g/encoding/ghash"
    "g/os/gfilespace"
    "sync"
    "sync/atomic"
    "bytes"
)

const (
    gDEFAULT_PART_SIZE       = 100000                   // 默认哈希表分区大小
    gMAX_KEY_SIZE            = 0xFF                     // 键名最大长度(255byte)
    gMAX_VALUE_SIZE          = 0xFFFFFF                 // 键值最大长度(16MB)
    gMETA_ITEM_SIZE          = 17                       // 元数据单项大小(byte)
    gMAX_META_LIST_SIZE      = 65535*gMETA_ITEM_SIZE    // 阶数，元数据列表最大大小(byte)
    gINDEX_BUCKET_SIZE       = 7                        // 索引文件数据块大小(byte)
    //gMETA_BUCKET_SIZE        = 5*gMETA_ITEM_SIZE        // 元数据数据分块大小(byte, 值越大，数据增长时占用的空间越大)
    gMETA_BUCKET_SIZE        = 1*gMETA_ITEM_SIZE        // 元数据数据分块大小(byte, 值越大，数据增长时占用的空间越大)
    //gDATA_BUCKET_SIZE        = 32                       // 数据分块大小(byte, 值越大，数据增长时占用的空间越大)
    gDATA_BUCKET_SIZE        = 4                       // 数据分块大小(byte, 值越大，数据增长时占用的空间越大)
    gFILE_POOL_CACHE_TIMEOUT = 60                       // 文件指针池缓存时间(秒)
    gCACHE_DEFAULT_TIMEOUT   = 10000                    // gcache默认缓存时间(毫秒)
    gAUTO_SAVING_TIMEOUT     = 100                      // 自动同步到磁盘的时间(毫秒)
    gAUTO_COMPACTING_TIMEOUT = 10                     // 自动进行数据整理的时间(毫秒)
)

// KV数据库
type DB struct {
    mu      sync.RWMutex
    path    string            // 数据文件存放目录路径
    name    string            // 数据文件名

    ixfp    *gfilepool.Pool   // 索引文件打开指针池(用以高并发下的IO复用)
    mtfp    *gfilepool.Pool   // 元数据文件打开指针池(元数据，包含索引信息和部分数据信息)
    dbfp    *gfilepool.Pool   // 数据文件打开指针池
    mtsp    *gfilespace.Space // 元数据文件碎片管理
    dbsp    *gfilespace.Space // 数据文件碎片管理器
    memt    *MemTable         // MemTable
    cached  int32             // 是否开启缓存功能(可动态开启/关闭)
    closed  int32             // 数据库是否关闭，以便异步线程进行判断处理
}

// 索引项
type Index struct {
    start  int64   // 索引开始位置
    end    int64   // 索引结束位置
    inc    int     // 分区，不同深度的分区增量会不同，准确的分区数需要和基本分区数进行累加
}

// 元数据项
type Meta struct {
    start  int64  // 开始位置
    end    int64  // 结束位置
    cap    int    // 列表分配长度(byte)
    size   int    // 列表真实长度(byte)
    buffer []byte // 数据项列表([]byte)
    match  int    // 是否在查找中匹配结果(-2, -1, 0, 1)
    index  int    // (匹配时有效, match=true)列表匹配的索引位置
}

// 数据项
type Data struct {
    start  int64  // 数据文件中的开始地址
    end    int64  // 数据文件中的结束地址
    cap    int   // 数据允许存放的的最大长度（用以修改对比）
    size   int   // klen + vlen
    klen   int   // 键名大小
    vlen   int   // 键值大小(byte)
}

// KV数据检索记录
type Record struct {
    hash64    uint    // 64位的hash code
    key       []byte  // 键名
    value     []byte  // 键值
    index     Index
    meta      Meta
    data      Data
}

// 创建一个KV数据库
func New(path, name string) (*DB, error) {
    path = strings.TrimRight(path, gfile.Separator)
    if name == "" {
        name = "gkvdb"
    }
    if !gfile.Exists(path) {
        if err := gfile.Mkdir(path); err != nil {
            return nil, err
        }
    }

    // 目录权限检测
    if !gfile.IsWritable(path) {
        return nil, errors.New(path + " is not writable")
    }
    db := &DB {
        path   : path,
        name   : name,
        cached : 0,
    }
    db.memt = newMemTable(db)

    // 索引/数据文件权限检测
    ixpath := db.getIndexFilePath()
    mtpath := db.getMetaFilePath()
    dbpath := db.getDataFilePath()
    fspath := db.getSpaceFilePath()
    if gfile.Exists(ixpath) && (!gfile.IsWritable(ixpath) || !gfile.IsReadable(ixpath)){
        return nil, errors.New("permission denied to index file: " + ixpath)
    }
    if gfile.Exists(mtpath) && (!gfile.IsWritable(mtpath) || !gfile.IsReadable(mtpath)){
        return nil, errors.New("permission denied to meta file: " + mtpath)
    }
    if gfile.Exists(dbpath) && (!gfile.IsWritable(dbpath) || !gfile.IsReadable(dbpath)){
        return nil, errors.New("permission denied to data file: " + dbpath)
    }
    if gfile.Exists(fspath) && (!gfile.IsWritable(fspath) || !gfile.IsReadable(fspath)){
        return nil, errors.New("permission denied to space file: " + fspath)
    }

    // 创建文件指针池
    db.ixfp = gfilepool.New(ixpath, os.O_RDWR|os.O_CREATE, gFILE_POOL_CACHE_TIMEOUT)
    db.mtfp = gfilepool.New(mtpath, os.O_RDWR|os.O_CREATE, gFILE_POOL_CACHE_TIMEOUT)
    db.dbfp = gfilepool.New(dbpath, os.O_RDWR|os.O_CREATE, gFILE_POOL_CACHE_TIMEOUT)
    // 初始化索引文件内容
    if gfile.Size(ixpath) == 0 {
        gfile.PutBinContents(ixpath, make([]byte, gINDEX_BUCKET_SIZE*gDEFAULT_PART_SIZE))
    }

    // 初始化相关服务
    db.initFileSpace()
    db.startAutoSavingLoop()
    db.startAutoCompactingLoop()
    return db, nil
}

func (db *DB) getIndexFilePath() string {
    return db.path + gfile.Separator + db.name + ".ix"
}

func (db *DB) getMetaFilePath() string {
    return db.path + gfile.Separator + db.name + ".mt"
}

func (db *DB) getDataFilePath() string {
    return db.path + gfile.Separator + db.name + ".db"
}

func (db *DB) getSpaceFilePath() string {
    return db.path + gfile.Separator + db.name + ".fs"
}

func (db *DB) isCacheEnabled() bool {
    return atomic.LoadInt32(&db.cached) > 0
}

func (db *DB) setCache(v int32) {
    atomic.StoreInt32(&db.cached, v)
}

func (db *DB) close() {
    db.ixfp.Close()
    db.mtfp.Close()
    db.dbfp.Close()
    atomic.StoreInt32(&db.closed, 1)
}

func (db *DB) isClosed() bool {
    return atomic.LoadInt32(&db.closed) > 0
}

// 根据元数据的size计算cap
func (db *DB) getMetaCapBySize(size int) int {
    if size > 0 && size%gMETA_BUCKET_SIZE != 0 {
        return size + gMETA_BUCKET_SIZE - size%gMETA_BUCKET_SIZE
    }
    return size
}

// 根据数据的size计算cap
func (db *DB) getDataCapBySize(size int) int {
    if size > 0 && size%gDATA_BUCKET_SIZE != 0 {
        return size + gDATA_BUCKET_SIZE - size%gDATA_BUCKET_SIZE
    }
    return size
}

// 计算关键字的hash code，使用64位哈希函数
func (db *DB) getHash64(key []byte) uint64 {
    return ghash.BKDRHash64(key)
}

// 获得索引信息，这里涉及到重复分区时索引的深度查找
func (db *DB) getIndexInfoByRecord(record *Record) error {
    pf, err := db.ixfp.File()
    if err != nil {
        return err
    }
    defer pf.Close()

    record.index.start = int64(record.hash64%gDEFAULT_PART_SIZE)*gINDEX_BUCKET_SIZE
    record.index.end   = record.index.start + gINDEX_BUCKET_SIZE
    for {
        if buffer := gfile.GetBinContentByTwoOffsets(pf.File(), record.index.start, record.index.end); buffer != nil {
            bits     := gbinary.DecodeBytesToBits(buffer)
            start    := int64(gbinary.DecodeBits(bits[0 : 36]))
            rehashed := uint(gbinary.DecodeBits(bits[55 : 56]))
            if rehashed == 0 {
                record.meta.start = start*gMETA_BUCKET_SIZE
                record.meta.size  = int(gbinary.DecodeBits(bits[36 : 55]))*gMETA_ITEM_SIZE
                record.meta.cap   = db.getMetaCapBySize(record.meta.size)
                record.meta.end   = record.meta.start + int64(record.meta.size)
                break
            } else {
                partition          := gDEFAULT_PART_SIZE + record.index.inc
                record.index.inc    = int(gbinary.DecodeBits(bits[36 : 55]))
                record.index.start  = start*gINDEX_BUCKET_SIZE + int64(record.hash64%uint(partition))*gINDEX_BUCKET_SIZE
                record.index.end    = record.index.start + gINDEX_BUCKET_SIZE
            }
        } else {
            return errors.New("index not found")
        }
    }
    return nil
}

// 获得元数据信息，对比hash64和关键字长度
func (db *DB) getDataInfoByRecord(record *Record) error {
    pf, err := db.mtfp.File()
    if err != nil {
        return err
    }
    defer pf.Close()

    if record.meta.buffer = gfile.GetBinContentByTwoOffsets(pf.File(), record.meta.start, record.meta.end); record.meta.buffer != nil {
        // 二分查找
        min := 0
        max := len(record.meta.buffer)/gMETA_ITEM_SIZE - 1
        mid := 0
        cmp := -2
        for {
            if cmp == 0 || min > max {
                break
            }
            for {
                // 首先对比哈希值
                mid     = int((min + max) / 2)
                buffer := record.meta.buffer[mid*gMETA_ITEM_SIZE : mid*gMETA_ITEM_SIZE + gMETA_ITEM_SIZE]
                bits   := gbinary.DecodeBytesToBits(buffer)
                hash64 := gbinary.DecodeBits(bits[0 : 64])
                if record.hash64 < hash64 {
                    max = mid - 1
                    cmp = -1
                } else if record.hash64 > hash64 {
                    min = mid + 1
                    cmp = 1
                } else {
                    // 其次对比键名长度
                    klen := int(gbinary.DecodeBits(bits[64 : 72]))
                    if len(record.key) < klen {
                        max = mid - 1
                        cmp = -1
                    } else if len(record.key) > klen {
                        min = mid + 1
                        cmp = 1
                    } else {
                        // 最后对比完整键名
                        vlen    := int(gbinary.DecodeBits(bits[72 : 96]))
                        dbstart := int64(gbinary.DecodeBits(bits[96 : 136]))*gDATA_BUCKET_SIZE
                        dbsize  := klen + vlen + 1
                        dbend   := dbstart + int64(dbsize)
                        if data := db.getDataByOffset(dbstart, dbend); data != nil {
                            if cmp = bytes.Compare(record.key, data[1 : 1 + klen]); cmp == 0 {
                                record.value       = data[1 + klen:]
                                record.data.klen   = klen
                                record.data.vlen   = vlen
                                record.data.size   = dbsize
                                record.data.cap    = db.getDataCapBySize(dbsize)
                                record.data.start  = dbstart
                                record.data.end    = dbend
                                break
                            }
                        }
                    }
                }
                if cmp == 0 || min > max {
                    break
                }
            }
        }
        record.meta.index = mid*gMETA_ITEM_SIZE
        record.meta.match = cmp
    }
    return nil
}

// 查询检索信息
func (db *DB) getRecordByKey(key []byte) (*Record, error) {
    record := &Record {
        hash64  : uint(db.getHash64(key)),
        key     : key,
    }
    record.meta.match = -2

    // 查询索引信息
    if err := db.getIndexInfoByRecord(record); err != nil {
        return record, err
    }

    // 查询数据信息
    if record.meta.end > 0 {
        if err := db.getDataInfoByRecord(record); err != nil {
            return record, err
        }
    }
    return record, nil
}

// 查询数据信息键值
func (db *DB) getDataByOffset(start, end int64) []byte {
    if end > 0 {
        pf, err := db.dbfp.File()
        if err != nil {
            return nil
        }
        defer pf.Close()
        buffer := gfile.GetBinContentByTwoOffsets(pf.File(), start, end)
        if buffer != nil {
            return buffer
        }
    }
    return nil
}

// 查询数据信息键值
func (db *DB) getValueByKey(key []byte) ([]byte, error) {
    record, err := db.getRecordByKey(key)
    if err != nil {
        return nil, err
    }

    if record == nil {
        return nil, nil
    }

    return record.value, nil
}

// 根据索引信息删除指定数据
func (db *DB) removeDataByRecord(record *Record) error {
    if err := db.removeDataFromDb(record); err != nil {
        return err
    }
    if err := db.removeDataFromMt(record); err != nil {
        return err
    }

    if err := db.removeDataFromIx(record); err != nil {
        return err
    }
    return nil
}

// 从数据文件中删除指定数据
func (db *DB) removeDataFromDb(record *Record) error {
    // 添加碎片
    db.addDbFileSpace(int(record.data.start), record.data.cap)
    return nil
}

// 从元数据中删除指定数据
func (db *DB) removeDataFromMt(record *Record) error {
    // 如果没有匹配到数据，那么也没必要执行删除了
    if record.meta.match != 0 {
        return nil
    }
    pf, err := db.mtfp.File()
    if err != nil {
        return err
    }
    defer pf.Close()

    record.meta.buffer = db.removeMeta(record.meta.buffer, record.meta.index)
    record.meta.size   = len(record.meta.buffer)
    if record.meta.size == 0 {
        // 如果列表被清空，那么添加整块空间到碎片管理器
        db.addMtFileSpace(int(record.meta.start), record.meta.cap)
    } else {
        if _, err = pf.File().WriteAt(record.meta.buffer, record.meta.start); err != nil {
            return err
        }
        // 如果列表分配大小比较实际大小超过bucket，那么进行空间切分，添加多余的空间到碎片管理器
        db.checkAndResizeMtCap(record)
    }
    return nil
}

// 从索引中删除指定数据
func (db *DB) removeDataFromIx(record *Record) error {
    return db.updateIndexByRecord(record)
}

// 检查并更新元数据分配大小与实际大小，如果有多余的空间，交给碎片管理器
func (db *DB) checkAndResizeMtCap(record *Record) {
    if int(record.meta.cap - record.meta.size) >= gMETA_BUCKET_SIZE {
        realcap := db.getMetaCapBySize(record.meta.size)
        diffcap := record.meta.cap - realcap
        if diffcap >= gMETA_BUCKET_SIZE {
            record.meta.cap = realcap
            db.addMtFileSpace(int(record.meta.start)+int(realcap), diffcap)
        }
    }
}

// 检查并更新数据分配大小与实际大小，如果有多余的空间，交给碎片管理器
func (db *DB) checkAndResizeDbCap(record *Record) {
    if int(record.data.cap - record.data.size) >= gDATA_BUCKET_SIZE {
        realcap := db.getDataCapBySize(record.data.size)
        diffcap := record.data.cap - realcap
        if diffcap >= gDATA_BUCKET_SIZE {
            record.data.cap = realcap
            db.addDbFileSpace(int(record.data.start)+int(realcap), diffcap)
        }
    }
}

// 插入一条KV数据
func (db *DB) insertDataByRecord(record *Record) error {
    record.data.klen = len(record.key)
    record.data.vlen = len(record.value)
    record.data.size = record.data.klen + record.data.vlen + 1

    // 写入数据文件
    if err := db.updateDataByRecord(record); err != nil {
        return err
    }

    // 写入元数据
    if err := db.updateMetaByRecord(record); err != nil {
        return err
    }

    // 根据record信息更新索引文件
    if err := db.updateIndexByRecord(record); err != nil {
        return err
    }

    // 判断是否需要重复分区
    db.checkDeepRehash(record)
    return nil
}

// 添加一项, cmp < 0往前插入，cmp >= 0往后插入
func (db *DB) saveMeta(slice []byte, buffer []byte, index int, cmp int) []byte {
    if cmp == 0 {
        copy(slice[index:], buffer)
        return slice
    }
    pos := index
    if cmp == -1 {
        // 添加到前面
    } else {
        // 添加到后面
        pos = index + gMETA_ITEM_SIZE
        if pos >= len(slice) {
            pos = len(slice)
        }
    }
    rear  := append([]byte{}, slice[pos : ]...)
    slice  = append(slice[0 : pos], buffer...)
    slice  = append(slice, rear...)
    return slice
}


// 删除一项
func (db *DB) removeMeta(slice []byte, index int) []byte {
    return append(slice[ : index], slice[index + gMETA_ITEM_SIZE : ]...)
}

// 将数据写入到数据文件中，并更新信息到record
func (db *DB) updateDataByRecord(record *Record) error {
    pf, err := db.dbfp.File()
    if err != nil {
        return err
    }
    defer pf.Close()
    // 判断是否额外分配键值存储空间
    if record.data.end <= 0 || record.data.cap < record.data.size {
        // 不用的空间添加到碎片管理器
        if record.data.end > 0 && record.data.cap > 0 {
            //fmt.Println("add db block", int(record.data.start), uint(record.data.cap))
            db.addDbFileSpace(int(record.data.start), record.data.cap)
        }
        // 重新计算所需空间
        if record.data.cap < record.data.size {
            for {
                record.data.cap += gDATA_BUCKET_SIZE
                if record.data.cap >= record.data.size {
                    break
                }
            }
        }
        record.data.start = db.getDbFileSpace(record.data.cap)
        record.data.end   = record.data.start + int64(record.data.size)
    }
    // vlen不够vcap的对末尾进行补0占位(便于文件末尾分配空间)
    buffer := make([]byte, 0)
    buffer  = append(buffer, byte(len(record.key)))
    buffer  = append(buffer, record.key...)
    buffer  = append(buffer, record.value...)
    for i := 0; i < int(record.data.cap - record.data.size); i++ {
        buffer = append(buffer, byte(0))
    }
    if _, err = pf.File().WriteAt(buffer, record.data.start); err != nil {
        return err
    }
    db.checkAndResizeDbCap(record)
    return nil
}

// 将数据写入到元数据文件中，并更新信息到record
func (db *DB) updateMetaByRecord(record *Record) error {
    pf, err := db.mtfp.File()
    if err != nil {
        return err
    }
    defer pf.Close()

    // 二进制打包
    bits := make([]gbinary.Bit, 0)
    bits  = gbinary.EncodeBits(bits, record.hash64,           64)
    bits  = gbinary.EncodeBits(bits, uint(record.data.klen),   8)
    bits  = gbinary.EncodeBits(bits, uint(record.data.vlen),  24)
    bits  = gbinary.EncodeBits(bits, uint(record.data.start/gDATA_BUCKET_SIZE), 40)
    // 数据列表打包(判断位置进行覆盖或者插入)
    record.meta.buffer = db.saveMeta(record.meta.buffer, gbinary.EncodeBitsToBytes(bits), record.meta.index, record.meta.match)
    record.meta.size   = len(record.meta.buffer)
    if record.meta.end <= 0 || record.meta.cap < record.meta.size {
        // 不用的空间添加到碎片管理器
        if record.meta.end > 0 && record.meta.cap > 0 {
            db.addMtFileSpace(int(record.meta.start), record.meta.cap)
        }
        // 重新计算所需空间
        if record.meta.cap < record.meta.size {
            for {
                record.meta.cap += gMETA_BUCKET_SIZE
                if record.meta.cap >= record.meta.size {
                    break
                }
            }
        }
        record.meta.start = db.getMtFileSpace(record.meta.cap)
        record.meta.end   = record.meta.start + int64(record.meta.cap)
    }

    //fmt.Println("meta write:", record.meta.start, record.meta.buffer)

    // size不够cap的对末尾进行补0占位(便于文件末尾分配空间)
    buffer := record.meta.buffer
    for i := 0; i < int(record.meta.cap - record.meta.size); i++ {
        buffer = append(buffer, byte(0))
    }

    if _, err = pf.File().WriteAt(buffer, record.meta.start); err != nil {
        return err
    }

    db.checkAndResizeMtCap(record)
    return nil
}

// 根据record更新索引信息
func (db *DB) updateIndexByRecord(record *Record) error {
    ixpf, err := db.ixfp.File()
    if err != nil {
        return err
    }
    defer ixpf.Close()

    var buffer []byte
    if record.meta.size > 0 {
        // 添加/修改/部分删除
        bits  := make([]gbinary.Bit, 0)
        bits   = gbinary.EncodeBits(bits, uint(record.meta.start/gMETA_BUCKET_SIZE),   36)
        bits   = gbinary.EncodeBits(bits, uint(record.meta.size/gMETA_ITEM_SIZE),      19)
        bits   = gbinary.EncodeBits(bits, 0,                                            1)
        buffer = gbinary.EncodeBitsToBytes(bits)
    } else {
        // 数据全部删除完，标记为0
        buffer = make([]byte, gINDEX_BUCKET_SIZE)
    }

    if _, err = ixpf.File().WriteAt(buffer, record.index.start); err != nil {
        return err
    }
    return nil
}


