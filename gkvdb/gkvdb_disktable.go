package gkvdb

import (
    "bytes"
    "errors"
    "gitee.com/johng/gf/g/container/gtype"
    "gitee.com/johng/gf/g/encoding/gbinary"
    "gitee.com/johng/gf/g/os/gcache"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gkvdb/gkvdb/gfilespace"
    "os"
    "sync"
)

// 数据表
type Table struct {
    mu     sync.RWMutex      // 并发互斥锁
    db     *DB               // 所属数据库
    name   string            // 数据表表名

    mtsp   *gfilespace.Space // 元数据文件碎片管理
    dbsp   *gfilespace.Space // 数据文件碎片管理器
    memt   *MemTable         // MemTable
    cache  *gcache.Cache     // 缓存管理对象
    closed *gtype.Bool       // 数据库是否关闭，以便异步线程进行判断处理
}

// 索引项
type _Index struct {
    start  int64   // 索引开始位置
    end    int64   // 索引结束位置
    size   int     // 分区大小
}

// 元数据项
type _Meta struct {
    start  int64  // 开始位置
    end    int64  // 结束位置
    cap    int    // 列表分配长度(byte)
    size   int    // 列表真实长度(byte)
    buffer []byte // 数据项列表([]byte)
    match  int    // 是否在查找中匹配结果(-2, -1, 0, 1)
    index  int    // (匹配时有效, match=0)列表匹配的索引位置
}

// 数据项
type _Data struct {
    start  int64  // 数据文件中的开始地址
    end    int64  // 数据文件中的结束地址
    cap    int    // 数据允许存放的的最大长度（用以修改对比）
    size   int    // klen + vlen
    klen   int    // 键名大小
    vlen   int    // 键值大小(byte)
}

// KV数据检索记录
type _Record struct {
    hash64    uint    // 64位的hash code
    key       []byte  // 键名
    value     []byte  // 键值
    index     _Index
    meta      _Meta
    data      _Data
}

// 获取数据表对象，如果表名已存在，那么返回已存在的表对象
func (db *DB) Table(name string) (*Table, error) {
    if v := db.tables.Get(name); v != nil {
        return v.(*Table), nil
    }

    if table, err := db.newTable(name); err == nil {
        return table, nil
    } else {
        return nil, err
    }
}

// 新建表或者读取现有表
func (db *DB) newTable(name string) (*Table, error) {
    // 初始化数据表信息
    table := &Table{
        db     : db,
        name   : name,
        closed : gtype.NewBool(),
    }
    table.memt = table.newMemTable()

    // 索引/数据文件权限检测
    ixpath := table.getIndexFilePath()
    mtpath := table.getMetaFilePath()
    dbpath := table.getDataFilePath()
    if gfile.Exists(ixpath) && (!gfile.IsWritable(ixpath) || !gfile.IsReadable(ixpath)){
        return nil, errors.New("permission denied to index file: " + ixpath)
    }
    if gfile.Exists(mtpath) && (!gfile.IsWritable(mtpath) || !gfile.IsReadable(mtpath)){
        return nil, errors.New("permission denied to meta file: " + mtpath)
    }
    if gfile.Exists(dbpath) && (!gfile.IsWritable(dbpath) || !gfile.IsReadable(dbpath)){
        return nil, errors.New("permission denied to data file: " + dbpath)
    }

    // 数据表缓存对象
    table.cache = gcache.New()

    // 初始化索引文件内容
    if gfile.Size(ixpath) == 0 {
        gfile.PutBinContents(ixpath, make([]byte, gINDEX_BUCKET_SIZE*gDEFAULT_PART_SIZE))
    }
    // 初始化相关服务
    table.initFileSpace()
    go table.startAutoCompactingLoop()

    // 保存数据表对象指针到全局数据库对象中
    table.db.tables.Set(name, table)
    return table, nil
}

// 关闭数据库链接，释放资源
func (table *Table) Close() {
    table.closed.Set(true)
}

// 索引文件
func (table *Table) getIndexFilePath() string {
    return table.db.path + gfile.Separator + table.name + ".ix"
}

// 元数据文件
func (table *Table) getMetaFilePath() string {
    return table.db.path + gfile.Separator + table.name + ".mt"
}

// 数据文件
func (table *Table) getDataFilePath() string {
    return table.db.path + gfile.Separator + table.name + ".db"
}

// 获得索引文件打开指针
func (table *Table) getIndexFilePointer() (*os.File, error) {
    return os.OpenFile(table.getIndexFilePath(), os.O_RDWR|os.O_CREATE, 0755)
}

// 获得元数据文件打开指针
func (table *Table) getMetaFilePointer() (*os.File, error) {
    return os.OpenFile(table.getMetaFilePath(), os.O_RDWR|os.O_CREATE, 0755)
}

// 获得索引文件打开指针
func (table *Table) getDataFilePointer() (*os.File, error) {
    return os.OpenFile(table.getDataFilePath(), os.O_RDWR|os.O_CREATE, 0755)
}

// 磁盘查询
func (table *Table) get(key []byte) []byte {
    ckey := "value_cache_" + string(key)
    if v := table.cache.Get(ckey); v != nil {
        return v.([]byte)
    }
    table.mu.RLock()
    defer table.mu.RUnlock()

    value, _ := table.getValueByKey(key)
    table.cache.Set(ckey, value, gCACHE_DEFAULT_TIMEOUT)
    return value
}

// 磁盘保存
func (table *Table) set(key []byte, value []byte) error {
    defer table.cache.Remove("value_cache_" + string(key))

    table.mu.Lock()
    defer table.mu.Unlock()

    // 查询索引信息
    record, err := table.getRecordByKey(key)
    if err != nil {
        return err
    }

    // 值未改变不用重写
    if record.value != nil && bytes.Compare(value, record.value) == 0 {
        return nil
    }

    // 写入数据文件，并更新record信息
    record.value = value
    if err := table.insertDataByRecord(record); err != nil {
        return errors.New("inserting data error: " + err.Error())
    }
    return nil
}


// 磁盘删除
func (table *Table) remove(key []byte) error {
    defer table.cache.Remove("value_cache_" + string(key))

    table.mu.Lock()
    defer table.mu.Unlock()

    // 查询索引信息
    record, err := table.getRecordByKey(key)
    if err != nil {
        return err
    }
    // 如果找到匹配才执行删除操作
    if record.meta.match == 0 {
        return table.removeDataByRecord(record)
    }
    return nil
}

// 遍历，注意该方法遍历只针对磁盘化后的数据，并且不包括中间binlog数据
// 该遍历会依次按照ix、mt、db文件进行遍历，并检测数据完整性，不完整的数据不会返回
func (table *Table) items(max int, m map[string][]byte) map[string][]byte {
    table.mu.RLock()
    defer table.mu.RUnlock()

    mtpf, err := table.getMetaFilePointer()
    if err != nil {
        return nil
    }
    defer mtpf.Close()

    dbpf, err := table.getDataFilePointer()
    if err != nil {
        return nil
    }
    defer dbpf.Close()

    ixbuffer := gfile.GetBinContents(table.getIndexFilePath())
    for i := 0; i < len(ixbuffer); i += gINDEX_BUCKET_SIZE {
        bits := gbinary.DecodeBytesToBits(ixbuffer[i: i+gINDEX_BUCKET_SIZE])
        if gbinary.DecodeBits(bits[55: 56]) != 0 {
            continue
        }
        mtindex := int(gbinary.DecodeBits(bits[ 0 : 36]))*gMETA_BUCKET_SIZE
        mtsize  := int(gbinary.DecodeBits(bits[36 : 55]))*gMETA_ITEM_SIZE
        // 如果不存在元数据，或者元数据包含在碎片中，那么忽略
        if mtsize == 0 || table.mtsp.Contains(mtindex, mtsize) {
            continue
        }
        if mtbuffer := gfile.GetBinContentsByTwoOffsets(mtpf, int64(mtindex), int64(mtindex + mtsize)); len(mtbuffer) > 0 {
            for i := 0; i < len(mtbuffer); i += gMETA_ITEM_SIZE {
                if table.mtsp.Contains(int(mtindex) + i, gMETA_ITEM_SIZE) {
                    continue
                }
                buffer := mtbuffer[i : i + gMETA_ITEM_SIZE]
                bits   := gbinary.DecodeBytesToBits(buffer)
                klen   := int(gbinary.DecodeBits(bits[64 : 72]))
                vlen   := int(gbinary.DecodeBits(bits[72 : 96]))
                if klen > 0 && vlen > 0 {
                    dbstart := int64(gbinary.DecodeBits(bits[96 : 136]))*gDATA_BUCKET_SIZE
                    dbend   := dbstart + int64(klen + vlen) + 1
                    data    := gfile.GetBinContentsByTwoOffsets(dbpf, dbstart, dbend)
                    keyb    := data[1 : 1 + klen]
                    key     := string(keyb)
                    m[key]   = data[1 + klen : ]
                    if len(m) == max {
                        return m
                    }
                }
            }
        }
    }

    return m
}

// 获得索引信息，这里涉及到重复分区时索引的深度查找
func (table *Table) getIndexInfoByRecord(record *_Record) error {
    pf, err := table.getIndexFilePointer()
    if err != nil {
        return err
    }
    defer pf.Close()

    record.index.start = int64(record.hash64%gDEFAULT_PART_SIZE)*gINDEX_BUCKET_SIZE
    record.index.end   = record.index.start + gINDEX_BUCKET_SIZE
    for {
        if buffer := gfile.GetBinContentsByTwoOffsets(pf, record.index.start, record.index.end); buffer != nil {
            bits     := gbinary.DecodeBytesToBits(buffer)
            start    := int64(gbinary.DecodeBits(bits[0 : 36]))
            rehashed := uint(gbinary.DecodeBits(bits[55 : 56]))
            if rehashed == 0 {
                record.meta.start = start*gMETA_BUCKET_SIZE
                record.meta.size  = int(gbinary.DecodeBits(bits[36 : 55]))*gMETA_ITEM_SIZE
                record.meta.cap   = getMetaCapBySize(record.meta.size)
                record.meta.end   = record.meta.start + int64(record.meta.size)
                break
            } else {
                partition          := record.index.size
                record.index.size   = int(gbinary.DecodeBits(bits[36 : 55]))
                record.index.start  = start*gINDEX_BUCKET_SIZE + int64(record.hash64%uint(partition))*gINDEX_BUCKET_SIZE
                record.index.end    = record.index.start + gINDEX_BUCKET_SIZE
            }
        } else {
            // index文件必定有值，即使数据不存在，那么也会有初始化的空值
            return errors.New("index not found")
        }
    }
    return nil
}

// 获得元数据信息，对比hash64和关键字长度
func (table *Table) getDataInfoByRecord(record *_Record) error {
    pf, err := table.getMetaFilePointer()
    if err != nil {
        return err
    }
    defer pf.Close()

    if record.meta.buffer = gfile.GetBinContentsByTwoOffsets(pf, record.meta.start, record.meta.end); record.meta.buffer != nil {
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
                hash64 := gbinary.DecodeBitsToUint(bits[0 : 64])
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
                        if data := table.getDataByOffset(dbstart, dbend); data != nil {
                            //fmt.Println(hash64, record.hash64)
                            //fmt.Println(string(record.key), string(data[1 : 1 + klen]))
                            if cmp = bytes.Compare(record.key, data[1 : 1 + klen]); cmp == 0 {
                                record.value       = data[1 + klen:]
                                record.data.klen   = klen
                                record.data.vlen   = vlen
                                record.data.size   = dbsize
                                record.data.cap    = getDataCapBySize(dbsize)
                                record.data.start  = dbstart
                                record.data.end    = dbend
                                break
                            }
                        } else {
                            return nil
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
func (table *Table) getRecordByKey(key []byte) (*_Record, error) {
    record := &_Record {
        hash64  : uint(getHash64(key)),
        key     : key,
    }
    record.meta.match = -2

    // 查询索引信息
    if err := table.getIndexInfoByRecord(record); err != nil {
        return record, err
    }

    // 查询数据信息
    if record.meta.end > 0 {
        if err := table.getDataInfoByRecord(record); err != nil {
            return record, err
        }
    }
    return record, nil
}

// 查询数据信息键值
func (table *Table) getDataByOffset(start, end int64) []byte {
    if end > 0 {
        pf, err := table.getDataFilePointer()
        if err != nil {
            return nil
        }
        defer pf.Close()
        buffer := gfile.GetBinContentsByTwoOffsets(pf, start, end)
        if buffer != nil {
            return buffer
        }
    }
    return nil
}

// 查询数据信息键值
func (table *Table) getValueByKey(key []byte) ([]byte, error) {
    record, err := table.getRecordByKey(key)
    if err != nil {
        return nil, err
    }

    if record == nil {
        return nil, nil
    }

    return record.value, nil
}

// 根据索引信息删除指定数据
// 只需要更新元数据信息即可(为保证高可用这里依旧采用新增数据方式进行更新)，旧有数据回收进碎片管理器
func (table *Table) removeDataByRecord(record *_Record) error {
    // 保存查询记录对象，以便处理碎片
    orecord := *record
    // 优先从元数据中剔除掉，成功之后数据便不完整，即使后续操作失败，该数据也会被识别为碎片
    if err := table.removeDataFromMt(record); err != nil {
        return err
    }
    // 其次更新索引信息
    if err := table.removeDataFromIx(record); err != nil {
        return err
    }
    // 数据删除操作执行成功之后，才将旧数据添加进入碎片管理器
    table.addMtFileSpace(int(orecord.meta.start), orecord.meta.cap)
    table.addDbFileSpace(int(orecord.data.start), orecord.data.cap)
    return nil
}

// 从元数据中删除指定数据
func (table *Table) removeDataFromMt(record *_Record) error {
    record.value       = nil
    record.meta.buffer = table.removeMeta(record.meta.buffer, record.meta.index)
    record.meta.size   = len(record.meta.buffer)
    return table.saveMetaByRecord(record)
}

// 从索引中删除指定数据
func (table *Table) removeDataFromIx(record *_Record) error {
    return table.saveIndexByRecord(record)
}

// 写入一条KV数据
func (table *Table) insertDataByRecord(record *_Record) error {
    record.data.klen = len(record.key)
    record.data.vlen = len(record.value)
    record.data.size = record.data.klen + record.data.vlen + 1

    // 保存查询记录对象，以便处理碎片
    orecord := *record

    // 写入数据文件
    if err := table.saveDataByRecord(record); err != nil {
        return err
    }

    // 写入元数据
    if err := table.saveMetaByRecord(record); err != nil {
        return err
    }

    // 根据record信息更新索引文件
    if err := table.saveIndexByRecord(record); err != nil {
        return err
    }

    // 数据写入操作执行成功之后，才将旧数据添加进入碎片管理器
    table.addMtFileSpace(int(orecord.meta.start), orecord.meta.cap)
    table.addDbFileSpace(int(orecord.data.start), orecord.data.cap)

    // 判断是否需要DRH
    return table.checkDeepRehash(record)
}

// 添加一项, cmp < 0往前插入，cmp >= 0往后插入
func (table *Table) saveMeta(slice []byte, buffer []byte, index int, cmp int) []byte {
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
func (table *Table) removeMeta(slice []byte, index int) []byte {
    return append(slice[ : index], slice[index + gMETA_ITEM_SIZE : ]...)
}

// 将数据写入到数据文件中，并更新信息到record
func (table *Table) saveDataByRecord(record *_Record) error {
    pf, err := table.getDataFilePointer()
    if err != nil {
        return err
    }
    defer pf.Close()
    // 为保证高可用，每一次都是额外分配键值存储空间，重新计算cap
    record.data.cap   = getDataCapBySize(record.data.size)
    record.data.start = table.getDbFileSpace(record.data.cap)
    record.data.end   = record.data.start + int64(record.data.size)

    // vlen不够vcap的对末尾进行补0占位(便于文件末尾分配空间)
    buffer := make([]byte, 0)
    buffer  = append(buffer, byte(len(record.key)))
    buffer  = append(buffer, record.key...)
    buffer  = append(buffer, record.value...)
    for i := 0; i < int(record.data.cap - record.data.size); i++ {
        buffer = append(buffer, byte(0))
    }
    if _, err = pf.WriteAt(buffer, record.data.start); err != nil {
        return err
    }

    return nil
}

// 将数据写入到元数据文件中，并更新信息到record
// 写入|删除
func (table *Table) saveMetaByRecord(record *_Record) error {
    pf, err := table.getMetaFilePointer()
    if err != nil {
        return err
    }
    defer pf.Close()

    // 当record.value==nil时表示删除，否则表示写入
    if record.value != nil {
        // 二进制打包
        bits := make([]gbinary.Bit, 0)
        bits  = gbinary.EncodeBitsWithUint(bits, record.hash64,                    64)
        bits  = gbinary.EncodeBits(bits, record.data.klen,                          8)
        bits  = gbinary.EncodeBits(bits, record.data.vlen,                         24)
        bits  = gbinary.EncodeBits(bits, int(record.data.start/gDATA_BUCKET_SIZE), 40)
        // 数据列表打包(判断位置进行覆盖或者插入)
        record.meta.buffer = table.saveMeta(record.meta.buffer, gbinary.EncodeBitsToBytes(bits), record.meta.index, record.meta.match)
        record.meta.size   = len(record.meta.buffer)
    }

    if record.meta.size > 0 {
        // 为保证高可用，每一次都是额外分配键值存储空间，重新计算cap
        record.meta.cap    = getMetaCapBySize(record.meta.size)
        record.meta.start  = table.getMtFileSpace(record.meta.cap)
        record.meta.end    = record.meta.start + int64(record.meta.size)
    }

    // size不够cap的对末尾进行补0占位(便于文件末尾分配空间)
    buffer := record.meta.buffer
    for i := 0; i < int(record.meta.cap - record.meta.size); i++ {
        buffer = append(buffer, byte(0))
    }

    if _, err = pf.WriteAt(buffer, record.meta.start); err != nil {
        return err
    }

    return nil
}

// 根据record更新索引信息
func (table *Table) saveIndexByRecord(record *_Record) error {
    ixpf, err := table.getIndexFilePointer()
    if err != nil {
        return err
    }
    defer ixpf.Close()

    var buffer []byte
    if record.meta.size > 0 {
        // 添加/修改/部分删除
        bits  := make([]gbinary.Bit, 0)
        bits   = gbinary.EncodeBits(bits, int(record.meta.start/gMETA_BUCKET_SIZE),   36)
        bits   = gbinary.EncodeBits(bits, record.meta.size/gMETA_ITEM_SIZE,           19)
        bits   = gbinary.EncodeBits(bits, 0,                                           1)
        buffer = gbinary.EncodeBitsToBytes(bits)
    } else {
        // 数据全部删除完，标记为0
        buffer = make([]byte, gINDEX_BUCKET_SIZE)
    }

    if _, err = ixpf.WriteAt(buffer, record.index.start); err != nil {
        return err
    }
    return nil
}

// 对数据库对应元数据列表进行重复分区
func (table *Table) checkDeepRehash(record *_Record) error {
    if record.meta.size < gMAX_META_LIST_SIZE {
        return nil
    }
    // 计算新创建的子哈希表的分区数，保证数据散列(分区后在同一请求处理中不再进行二次分区)
    size := record.index.size + 1
    pmap := make(map[int][]byte)
    done := true
    for {
        for i := 0; i < record.meta.size; i += gMETA_ITEM_SIZE {
            buffer := record.meta.buffer[i : i + gMETA_ITEM_SIZE]
            bits   := gbinary.DecodeBytesToBits(buffer)
            hash64 := gbinary.DecodeBitsToUint(bits[0 : 64])
            part   := int(hash64%uint(size))
            if _, ok := pmap[part]; !ok {
                pmap[part] = make([]byte, 0)
            }
            pmap[part] = append(pmap[part], buffer...)
            if len(pmap[part]) == gMAX_META_LIST_SIZE {
                done = false
                pmap = make(map[int][]byte)
                size++
                break
            }
        }
        if done {
            break
        } else {
            done = true
        }
    }

    // 计算元数据大小以便分配空间
    mtsize := 0
    for _, v := range pmap {
        mtsize += getMetaCapBySize(len(v))
    }
    // 生成写入的索引数据及元数据
    mtstart  := table.getMtFileSpace(mtsize)
    tmpstart := mtstart
    mtbuffer := make([]byte, 0)
    ixbuffer := make([]byte, 0)
    for i := 0; i < size; i++ {
        part := i
        if v, ok := pmap[part]; ok {
            bits     := make([]gbinary.Bit, 0)
            bits      = gbinary.EncodeBits(bits, int(tmpstart)/gMETA_BUCKET_SIZE,   36)
            bits      = gbinary.EncodeBits(bits, len(v)/gMETA_ITEM_SIZE,            19)
            bits      = gbinary.EncodeBits(bits, 0,                                  1)
            mtcap    := getMetaCapBySize(len(v))
            tmpstart += int64(mtcap)
            ixbuffer  = append(ixbuffer, gbinary.EncodeBitsToBytes(bits)...)
            mtbuffer  = append(mtbuffer, v...)
            for j := 0; j < int(mtcap) - len(v); j++ {
                mtbuffer = append(mtbuffer, byte(0))
            }
        } else {
            ixbuffer = append(ixbuffer, make([]byte, gINDEX_BUCKET_SIZE)...)
        }
    }

    // 写入重新分区后的元数据信息
    mtpf, err := table.getMetaFilePointer()
    if err != nil {
        return err
    }
    defer mtpf.Close()
    if _, err = mtpf.WriteAt(mtbuffer, mtstart); err != nil {
        return err
    }

    // 写入重新分区后的索引信息，需要写到末尾
    ixpf, err := table.getIndexFilePointer()
    if err != nil {
        return err
    }
    defer ixpf.Close()
    ixstart, err := ixpf.Seek(0, 2)
    if err != nil {
        return err
    }
    ixpf.WriteAt(ixbuffer, ixstart)

    // 修改老的索引信息
    bits := make([]gbinary.Bit, 0)
    bits  = gbinary.EncodeBits(bits, int(ixstart)/gINDEX_BUCKET_SIZE,  36)
    bits  = gbinary.EncodeBits(bits, size,                             19)
    bits  = gbinary.EncodeBits(bits, 1,                                 1)
    if _, err = ixpf.WriteAt(gbinary.EncodeBitsToBytes(bits), record.index.start); err != nil {
        return err
    }

    // 操作成功之后才会将旧空间添加进碎片管理
    table.addMtFileSpace(int(record.meta.start), record.meta.cap)

    return nil
}