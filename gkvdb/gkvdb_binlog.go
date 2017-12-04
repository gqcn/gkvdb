package gkvdb

import (
    "os"
    "sync"
    "errors"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/gfilepool"
    "gitee.com/johng/gf/g/container/glist"
    "gitee.com/johng/gf/g/encoding/gbinary"
    "sync/atomic"
)

// binlog操作对象
type BinLog struct {
    sync.RWMutex
    db    *DB              // 所属数据库
    fp    *gfilepool.Pool  // 文件指针池
    size  int32            // 当前的binlog大小
    queue *glist.SafeList  // 同步队列

}

// binlog写入项
type BinLogItem struct {
    txstart int64                        // 事务在binlog文件的开始位置
    datamap map[string]map[string][]byte // 事务数据
}

// 创建binlog对象
func newBinLog(db *DB) (*BinLog, error) {
    binlog := &BinLog{
        db    : db,
        queue : glist.NewSafeList(),
    }
    path := db.getBinLogFilePath()
    if gfile.Exists(path) && (!gfile.IsWritable(path) || !gfile.IsReadable(path)){
        return nil, errors.New("permission denied to binlog file: " + path)
    }
    binlog.fp = gfilepool.New(path, os.O_RDWR|os.O_CREATE, gFILE_POOL_CACHE_TIMEOUT)
    return binlog, nil
}

// 关闭binlog
func (binlog *BinLog) close() {
    binlog.fp.Close()
}

// 从binlog文件中恢复未同步数据到memtable中
// 内部会检测异常数据写入，并忽略异常数据，以便异常数据不会进入到数据库中
func (binlog *BinLog) initFromFile() {
    blbuffer := gfile.GetBinContents(binlog.db.getBinLogFilePath())
    if len(blbuffer) == 0 {
        return
    }
    binlog.size = int32(len(blbuffer))
    // 在异常数据下，需要花费更多的时间进行数据纠正(字节不断递增计算下一条正确的binlog位置)
    for i := 0; i < len(blbuffer); {
        buffer := blbuffer[i : i + 13]
        synced := int(gbinary.DecodeToInt8(buffer[0 : 1]))
        blsize := int(gbinary.DecodeToInt32(buffer[1 : 5]))
        if i + 13 + blsize + 8 > len(blbuffer) {
            i++
            continue
        }
        txidstart := gbinary.DecodeToInt64(buffer[5 : 13])
        txidend   := gbinary.DecodeToInt64(blbuffer[i + 13 + blsize : i + 13 + blsize + 8])
        if txidstart != txidend {
            //fmt.Println("invalid", i)
            i++
            continue
        } else {
            // 正常数据，同步到memtable中
            if synced == 0 {
                datamap := binlog.binlogBufferToDataMap(blbuffer[i + 13 : i + 13 + blsize])
                for n, m := range datamap {
                    if table, err := binlog.db.getTable(n); err == nil {
                        if err := table.memt.set(m); err != nil {
                            glog.Error(err)
                            os.Exit(1)
                        }
                    } else {
                        glog.Error(err)
                        os.Exit(1)
                    }
                }
                binlog.queue.PushFront(BinLogItem{int64(i), datamap})
            }
            i += 13 + blsize + 8
        }
    }
    //fmt.Println(gtime.Microsecond() - t1)
}

// 将二进制数据转换为事务对象
func (binlog *BinLog) binlogBufferToDataMap(buffer []byte) map[string]map[string][]byte {
    m := make(map[string]map[string][]byte)
    for i := 0; i < len(buffer); {
        bits  := gbinary.DecodeBytesToBits(buffer[i : i + 5])
        nlen  := int(gbinary.DecodeBits(bits[ 0 :  8]))
        klen  := int(gbinary.DecodeBits(bits[ 8 : 16]))
        vlen  := int(gbinary.DecodeBits(bits[16 : 40]))
        name  := buffer[i + 5 : i + 5 + nlen]
        key   := buffer[i + 5 + nlen : i + 5 + nlen + klen]
        value := buffer[i + 5 + nlen + klen : i + 5 + nlen + klen + vlen]
        if _, ok := m[string(name)]; !ok {
            m[string(name)] = make(map[string][]byte)
        }
        m[string(name)][string(key)] = value
        i += 5 + nlen + klen + vlen
    }
    return m
}

// 添加binlog到文件，支持批量添加
// 返回写入的文件开始位置，以及是否有错误
func (binlog *BinLog) writeByTx(tx *Transaction) error {
    buffer := make([]byte, 0)
    // 事务开始
    buffer  = append(buffer, gbinary.EncodeInt8(0)...)
    buffer  = append(buffer, gbinary.EncodeInt32(0)...)
    buffer  = append(buffer, gbinary.EncodeInt64(tx.id)...)
    // 数据列表
    blsize := 0
    for n, m := range tx.tables {
        for k, v := range m {
            bits   := make([]gbinary.Bit, 0)
            bits    = gbinary.EncodeBits(bits, uint(len(n)),   8)
            bits    = gbinary.EncodeBits(bits, uint(len(k)),   8)
            bits    = gbinary.EncodeBits(bits, uint(len(v)),  24)
            buffer  = append(buffer, gbinary.EncodeBitsToBytes(bits)...)
            buffer  = append(buffer, n...)
            buffer  = append(buffer, k...)
            buffer  = append(buffer, v...)
            blsize += 5 + len(n) + len(k) + len(v)
        }
    }

    // 事务结束
    buffer  = append(buffer, gbinary.EncodeInt64(tx.id)...)
    // 修改数据长度
    copy(buffer[1:], gbinary.EncodeInt32(int32(blsize)))

    // 从指针池获取
    blpf, err := binlog.fp.File()
    if err != nil {
        return err
    }
    defer blpf.Close()

    binlog.Lock()
    defer binlog.Unlock()

    // 写到文件末尾
    start, err := blpf.File().Seek(0, 2)
    if err != nil {
        return err
    }
    // 执行数据写入
    if _, err := blpf.File().WriteAt(buffer, start); err != nil {
        return err
    }

    // 再写内存表(分别写入到对应表的memtable中)
    for n, m := range tx.tables {
        if table, err := tx.db.getTable(n); err == nil {
            if err := table.memt.set(m); err != nil {
                os.Truncate(binlog.db.getBinLogFilePath(), start)
                return err
            }
        } else {
            os.Truncate(binlog.db.getBinLogFilePath(), start)
            return err
        }
    }

    // 添加到磁盘化队列
    if binlog.queue.PushFront(BinLogItem{start, tx.tables}) == nil {
        return errors.New("push binlog to sync queue failed")
    }

    // 增加binlog大小
    binlog.addSize(len(buffer))

    return nil
}

// 获取binlog大小
func (binlog *BinLog) getSize() int {
    return int(atomic.LoadInt32(&binlog.size))
}

// 增加binlog大小
func (binlog *BinLog) addSize(size int) {
    atomic.AddInt32(&binlog.size, int32(size))
}

// 写入磁盘，标识事务已经同步，在对应位置只写入1个字节
func (binlog *BinLog) markTxSynced(start int64) error {
    blpf, err := binlog.fp.File()
    if err != nil {
        return err
    }
    defer blpf.Close()

    binlog.Lock()
    defer binlog.Unlock()

    if _, err := blpf.File().WriteAt(gbinary.EncodeInt8(1), start); err != nil {
        return err
    }
    return nil
}
