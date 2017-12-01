package gkvdb

import (
    "os"
    "time"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/gcache"
    "gitee.com/johng/gf/g/encoding/gbinary"
    "gitee.com/johng/gf/g/os/glog"
)

// 数据文件自动整理
func (db *DB) startAutoCompactingLoop() {
    go func() {
        for !db.isClosed() {
            if err := db.autoCompactingData(); err != nil {
                glog.Error(err)
                time.Sleep(time.Minute)
            }
            if err := db.autoCompactingMeta(); err != nil {
                glog.Error(err)
                time.Sleep(time.Minute)
            }
            time.Sleep(gAUTO_COMPACTING_TIMEOUT*time.Millisecond)
        }
    }()
}

// 开启自动同步线程
func (db *DB) startAutoSyncingLoop() {
    go func() {
        for !db.isClosed() {
            if err := db.doAutoSyncing(); err != nil {
                glog.Error(err)
            }
            time.Sleep(gBINLOG_AUTO_SYNCING*time.Millisecond)
        }
    }()
}

// 自动同步binlog的数据到数据表中
func (db *DB) doAutoSyncing() error {
    key := "auto_syncing_cache_key_for_" + db.path + db.name
    if gcache.Get(key) != nil {
        return nil
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    // 如果没有可同步的数据，那么立即返回
    if db.binlog.queue.Len() == 0 {
        return nil
    }

    for {
        if v := db.binlog.queue.PopBack(); v != nil {
            item := v.(BinLogItem)
            for k, v := range item.datamap {
                if len(v) == 0 {
                    if err := db.remove([]byte(k)); err != nil {
                        db.binlog.queue.PushBack(item)
                        return err
                    }
                } else {
                    if err := db.set([]byte(k), v); err != nil {
                        db.binlog.queue.PushBack(item)
                        return err
                    }
                }
            }
            db.binlog.markTxSynced(item.txstart)
        } else {
            // 如果所有的事务数据已经同步完成，那么矫正binblog文件大小
            binlogPath := db.getBinLogFilePath()
            db.binlog.Lock()
            if gfile.Size(binlogPath) > 0 {
                db.memt.clear()
                os.Truncate(binlogPath, 0)
            }
            db.binlog.Unlock()
            break
        }

    }
    return nil
}

// 数据，将最大的空闲块依次往后挪，直到文件末尾，然后truncate文件
func (db *DB) autoCompactingData() error {
    key := "auto_compacting_data_cache_key_for_" + db.path + db.name
    if gcache.Get(key) != nil {
        return nil
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    db.mu.Lock()
    defer db.mu.Unlock()

    maxsize := db.getDbFileSpaceMaxSize()
    if maxsize < gAUTO_COMPACTING_MINSIZE {
        return nil
    }
    index := db.getDbFileSpace(maxsize)
    if index < 0 {
        return nil
    }
    dbsize  := gfile.Size(db.getDataFilePath())
    dbstart := index + int64(maxsize)
    if dbstart == dbsize {
        return os.Truncate(db.getDataFilePath(), int64(index))
    } else {
        dbpf, err := db.dbfp.File()
        if err != nil {
            return err
        }
        defer dbpf.Close()
        if buffer := gfile.GetBinContentByTwoOffsets(dbpf.File(), dbstart, dbstart + 1 + gMAX_KEY_SIZE); buffer != nil {
            klen := gbinary.DecodeToUint8(buffer[0 : 1])
            key  := buffer[1 : 1 + klen]
            record := &Record {
                hash64  : uint(db.getHash64(key)),
                key     : key,
            }
            // 查找对应的索引信息，并执行更新
            if err := db.getIndexInfoByRecord(record); err == nil {
                if record.meta.end > 0 {
                    if err := db.getDataInfoByRecord(record); err == nil {
                        record.data.start -= int64(maxsize)
                        record.data.cap   += maxsize
                        db.updateDataByRecord(record)
                        db.updateMetaByRecord(record)
                        db.updateIndexByRecord(record)
                    } else {
                        return err
                    }
                }
            } else {
                return err
            }
        }
    }
    return nil
}

// 元数据，将最大的空闲块依次往后挪，直到文件末尾，然后truncate文件
func (db *DB) autoCompactingMeta() error {
    key := "auto_compacting_meta_cache_key_for_" + db.path + db.name
    if gcache.Get(key) != nil {
        return nil
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    db.mu.Lock()
    defer db.mu.Unlock()

    maxsize := db.getMtFileSpaceMaxSize()
    if maxsize < gAUTO_COMPACTING_MINSIZE {
        return nil
    }
    index := db.getMtFileSpace(maxsize)
    if index < 0 {
        return nil
    }
    mtsize  := gfile.Size(db.getMetaFilePath())
    mtstart := index + int64(maxsize)
    if mtstart == mtsize {
        return os.Truncate(db.getMetaFilePath(), int64(index))
    } else {
        mtpf, err := db.mtfp.File()
        if err != nil {
            return err
        }
        defer mtpf.Close()
        // 找到对应空闲块下一条meta item数据
        if buffer := gfile.GetBinContentByTwoOffsets(mtpf.File(), mtstart, mtstart + gMETA_ITEM_SIZE); buffer != nil {
            bits   := gbinary.DecodeBytesToBits(buffer)
            hash64 := gbinary.DecodeBits(bits[0 : 64])
            record := &Record {
                hash64  : hash64,
            }
            // 查找对应的索引信息，并执行更新
            if err := db.getIndexInfoByRecord(record); err == nil {
                if mtbuffer := gfile.GetBinContentByTwoOffsets(mtpf.File(), record.meta.start, record.meta.end); mtbuffer != nil {
                    record.meta.start -= int64(maxsize)
                    record.meta.cap   += maxsize
                    if _, err = mtpf.File().WriteAt(mtbuffer, record.meta.start); err == nil {
                        db.updateIndexByRecord(record)
                        db.checkAndResizeMtCap(record)
                    } else {
                        return err
                    }
                }
            } else {
                return err
            }
        }
    }
    return nil
}
