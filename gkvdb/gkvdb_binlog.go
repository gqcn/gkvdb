package gkvdb

import (
    "bytes"
    "errors"
    "gitee.com/johng/gf/g/container/glist"
    "gitee.com/johng/gf/g/encoding/gbinary"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "math"
    "os"
    "sync"
    "sync/atomic"
    "time"
)

// binlog操作对象
type BinLog struct {
    sync.RWMutex                     // binlog文件互斥锁
    smu             sync.RWMutex     // binlog同步互斥锁
    db              *DB              // 所属数据库
    queue           *glist.List      // 同步打包数据队列
    queuesize       int32            // 队列大小限制(byte)，注意不是binlog文件大小，是未同步的队列数据大小
    syncEvents      chan struct{}    // 数据同步通知事件
    closeEvents     chan struct{}    // 数据库关闭事件
    limitFreeEvents chan struct{}    // 数据长度上限阻塞释放通知事件
}

// binlog写入项
type BinLogItem struct {
    size    int32                        // 数据项大小(byte)
    txstart int64                        // 事务在binlog文件的开始位置
    datamap map[string]map[string][]byte // 事务数据(可能有多个)
}

// 创建binlog对象
func newBinLog(db *DB) (*BinLog, error) {
    binlog := &BinLog{
        db              : db,
        queue           : glist.New(),
        syncEvents      : make(chan struct{}, math.MaxUint32),
        closeEvents     : make(chan struct{}, 2),
        limitFreeEvents : make(chan struct{}, 0),
    }
    path := db.getBinLogFilePath()
    if gfile.Exists(path) && (!gfile.IsWritable(path) || !gfile.IsReadable(path)){
        return nil, errors.New("permission denied to binlog file: " + path)
    }
    return binlog, nil
}

// 关闭binlog
func (binlog *BinLog) close() {
    binlog.closeEvents <- struct{}{}
}

// 从binlog文件中恢复未同步数据到memtable中
// 内部会检测异常数据写入，并忽略异常数据，以便异常数据不会进入到数据库中
// 这里是数据库初始化操作，如果产生错误，那么输出错误信息到终端
func (binlog *BinLog) initFromFile() {
    blbuffer := gfile.GetBinContents(binlog.db.getBinLogFilePath())
    if len(blbuffer) == 0 {
        return
    }
    // 在异常数据下，需要花费更多的时间进行数据纠正(字节不断递增计算下一条正确的binlog位置)
    for i := 0; i < len(blbuffer); {
        buffer := blbuffer[i : i + 13]
        blsize := int(gbinary.DecodeToInt32(buffer[1 : 5]))
        if blsize < 0 ||
            i + 13 + blsize + 8 > len(blbuffer) ||
            bytes.Compare(blbuffer[i + 5 : i + 13], blbuffer[i + 13 + blsize : i + 13 + blsize + 8]) != 0 {
            glog.Errorfln("binlog was corrupt, ignore index: %d\n", i)
            i++
        } else {
            // 正常数据，判断并同步到memtable中
            if gbinary.DecodeToInt8(buffer[0 : 1]) == 0 {
                datamap := binlog.binlogBufferToDataMap(blbuffer[i + 13 : i + 13 + blsize])
                for n, m := range datamap {
                    if table, err := binlog.db.Table(n); err == nil {
                        table.memt.set(m)
                    } else {
                        glog.Error(err)
                    }
                }
                binlog.queuesize += int32(blsize) + 13
                binlog.queue.PushFront(BinLogItem{int32(blsize) + 13, int64(i), datamap})
            }
            i += 13 + blsize + 8
        }
    }

    // 判断继续执行同步
    if binlog.queue.Len() > 0 {
        binlog.syncEvents <- struct{}{}
    }
}

// 将二进制数据转换为事务对象
func (binlog *BinLog) binlogBufferToDataMap(buffer []byte) map[string]map[string][]byte {
    datamap := make(map[string]map[string][]byte)
    for i := 0; i < len(buffer); {
        bits  := gbinary.DecodeBytesToBits(buffer[i : i + 5])
        nlen  := int(gbinary.DecodeBits(bits[ 0 :  8]))
        klen  := int(gbinary.DecodeBits(bits[ 8 : 16]))
        vlen  := int(gbinary.DecodeBits(bits[16 : 40]))
        name  := buffer[i + 5 : i + 5 + nlen]
        key   := buffer[i + 5 + nlen : i + 5 + nlen + klen]
        value := buffer[i + 5 + nlen + klen : i + 5 + nlen + klen + vlen]
        if _, ok := datamap[string(name)]; !ok {
            datamap[string(name)] = make(map[string][]byte)
        }
        datamap[string(name)][string(key)] = value
        i += 5 + nlen + klen + vlen
    }
    return datamap
}

// 是否binlog长度达到上限
func (binlog *BinLog) reachLengthLimit() bool {
    return atomic.LoadInt32(&binlog.queuesize) >= gBINLOG_MAX_SIZE
}

// 添加binlog到文件，支持批量添加
// 返回写入的文件开始位置，以及是否有错误
// 第二个参数表示是否强制写入到磁盘
func (binlog *BinLog) writeByTx(tx *Transaction, sync...bool) error {
    if binlog.reachLengthLimit() {
        <- binlog.limitFreeEvents
    }
    // 内容序列
    buffer := make([]byte, 0)
    // 事务开始
    buffer  = append(buffer, gbinary.EncodeInt8(0)...)
    buffer  = append(buffer, gbinary.EncodeInt32(0)...)
    buffer  = append(buffer, gbinary.EncodeInt64(tx.id)...)
    // 数据列表
    blsize := 0
    for ns, m := range tx.tables {
        for ks, v := range m {
            n      := []byte(ns)
            k      := []byte(ks)
            bits   := make([]gbinary.Bit, 0)
            bits    = gbinary.EncodeBits(bits, len(n),   8)
            bits    = gbinary.EncodeBits(bits, len(k),   8)
            bits    = gbinary.EncodeBits(bits, len(v),  24)
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
    blpf, err := binlog.db.getBinlogFilePointer()
    if err != nil {
        return err
    }
    defer blpf.Close()

    binlog.Lock()
    defer binlog.Unlock()

    // 写到文件末尾
    start, err := blpf.Seek(0, 2)
    if err != nil {
        return err
    }

    // 执行数据写入
    if _, err := blpf.WriteAt(buffer, start); err != nil {
        return err
    }
    // 是否执行文件sync
    if len(sync) > 0 && sync[0] {
        if err := blpf.Sync(); err != nil {
            return err
        }
    }

    // 再写内存表(分别写入到对应表的memtable中)
    for n, m := range tx.tables {
        if table, err := tx.db.Table(n); err == nil {
            table.memt.set(m)
        } else {
            os.Truncate(binlog.db.getBinLogFilePath(), start)
            return err
        }
    }

    // 添加到磁盘化队列
    binlog.queue.PushFront(BinLogItem{int32(blsize) + 13, start, tx.tables})
    // 增加数据队列长度记录
    atomic.AddInt32(&binlog.queuesize, int32(blsize) + 13)

    // 发送同步通知事件
    binlog.syncEvents <- struct{}{}
    return nil
}

// 写入磁盘，标识事务已经同步，在对应位置只写入1个字节
func (binlog *BinLog) markSynced(start int64) error {
    blpf, err := binlog.db.getBinlogFilePointer()
    if err != nil {
        return err
    }
    defer blpf.Close()

    binlog.Lock()
    defer binlog.Unlock()

    if _, err := blpf.WriteAt(gbinary.EncodeInt8(1), start); err != nil {
        return err
    }
    return nil
}

// 执行binlog同步
func (binlog *BinLog) sync() {
    if binlog.queue.Len() == 0 {
        return
    }
    // binlog互斥锁保证同时只有一个线程在运行
    binlog.smu.Lock()
    defer binlog.smu.Unlock()
    for {
        if v := binlog.queue.PopBack(); v != nil {
            wg   := sync.WaitGroup{}
            item := v.(BinLogItem)
            done := int32(0)
            // 一般不会为空
            if item.datamap == nil {
                continue
            }
            for n, m := range item.datamap {
                wg.Add(1)
                // 不同的数据表异步执行数据保存
                name := n
                data := m
                go func() {
                    defer wg.Done()
                    // 获取数据表对象
                    table, err := binlog.db.Table(name)
                    if err != nil {
                        atomic.StoreInt32(&done, -1)
                        glog.Error(err)
                        return
                    }
                    for k, v := range data {
                        if len(v) == 0 {
                            // 删除操作
                            if err := table.remove([]byte(k)); err != nil {
                                atomic.StoreInt32(&done, -1)
                                glog.Error(err)
                                return
                            }
                        } else {
                            // 写入操作(新增/修改)
                            if err := table.set([]byte(k), v); err != nil {
                                atomic.StoreInt32(&done, -1)
                                glog.Error(err)
                                return
                            }
                        }
                    }
                }()
            }
            wg.Wait()
            // 同步失败，重新推入队列
            if done < 0 {
                binlog.queue.PushBack(item)
                glog.Error("data sync failed, retry later")
                time.Sleep(time.Second)
            } else {
                binlog.markSynced(item.txstart)
                atomic.AddInt32(&binlog.queuesize, -item.size)
            }
        } else {
            // 将binlog文件锁起来，
            // 防止在文件大小矫正过程中内容发生改变
            binlog.Lock()
            // 必须要保证所有binlog已经同步完成才执行清空操作
            if atomic.LoadInt32(&binlog.queuesize) <= 0 && binlog.queue.Len() == 0 {
                // 清空数据库所有的表的缓存，由于该操作在binlog写锁内部执行，
                // binlog写入完成之后才能写memtable，因此这里不存在memtable在清理的过程中写入数据的问题
                binlog.db.tables.Iterator(func(k string, v interface{}) bool{
                    v.(*Table).memt.clear()
                    return true
                })
                atomic.StoreInt32(&binlog.queuesize, 0)
                os.Truncate(binlog.db.getBinLogFilePath(), 0)
            }
            binlog.Unlock()
            close(binlog.limitFreeEvents)
            binlog.limitFreeEvents = make(chan struct{}, 0)
            break
        }
    }
}