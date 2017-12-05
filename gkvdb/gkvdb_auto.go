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
func (table *Table) startAutoCompactingLoop() {
    go func() {
        for !table.isClosed() {
            if err := table.autoCompactingData(); err != nil {
                glog.Error(err)
                time.Sleep(time.Second)
            }
            if err := table.autoCompactingMeta(); err != nil {
                glog.Error(err)
                time.Sleep(time.Second)
            }
            time.Sleep(gAUTO_COMPACTING_TIMEOUT*time.Millisecond)
        }
    }()
}

// 开启自动同步线程
func (db *DB) startAutoSyncingLoop() {
    go func() {
        for !db.isClosed() {
            db.binlog.sync()
            time.Sleep(gBINLOG_AUTO_SYNCING*time.Millisecond)
        }
    }()
}


// 数据，将最大的空闲块依次往后挪，直到文件末尾，然后truncate文件
func (table *Table) autoCompactingData() error {
    key := "auto_compacting_data_cache_key_for_" + table.db.path + table.name
    if gcache.Get(key) != nil {
        return nil
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    table.mu.Lock()
    defer table.mu.Unlock()

    maxsize := table.getDbFileSpaceMaxSize()
    if maxsize < gAUTO_COMPACTING_MINSIZE {
        return nil
    }
    index := table.getDbFileSpace(maxsize)
    if index < 0 {
        return nil
    }
    dbpath  := table.getDataFilePath()
    dbsize  := gfile.Size(dbpath)
    dbstart := index + int64(maxsize)
    if dbstart == dbsize {
        return os.Truncate(dbpath, int64(index))
    } else {
        dbpf, err := table.dbfp.File()
        if err != nil {
            return err
        }
        defer dbpf.Close()
        if buffer := gfile.GetBinContentByTwoOffsets(dbpf.File(), dbstart, dbstart + 1 + gMAX_KEY_SIZE); buffer != nil {
            klen := gbinary.DecodeToUint8(buffer[0 : 1])
            key  := buffer[1 : 1 + klen]
            record := &Record {
                hash64  : uint(getHash64(key)),
                key     : key,
            }
            // 查找对应的索引信息，并执行更新
            if err := table.getIndexInfoByRecord(record); err == nil {
                if record.meta.end > 0 {
                    if err := table.getDataInfoByRecord(record); err == nil {
                        record.data.start -= int64(maxsize)
                        record.data.cap   += maxsize
                        table.updateDataByRecord(record)
                        table.updateMetaByRecord(record)
                        table.updateIndexByRecord(record)
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
func (table *Table) autoCompactingMeta() error {
    key := "auto_compacting_meta_cache_key_for_" + table.db.path + table.name
    if gcache.Get(key) != nil {
        return nil
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    table.mu.Lock()
    defer table.mu.Unlock()

    maxsize := table.getMtFileSpaceMaxSize()
    if maxsize < gAUTO_COMPACTING_MINSIZE {
        return nil
    }
    index := table.getMtFileSpace(maxsize)
    if index < 0 {
        return nil
    }
    mtsize  := gfile.Size(table.getMetaFilePath())
    mtstart := index + int64(maxsize)
    if mtstart == mtsize {
        return os.Truncate(table.getMetaFilePath(), int64(index))
    } else {
        mtpf, err := table.mtfp.File()
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
            if err := table.getIndexInfoByRecord(record); err == nil {
                if mtbuffer := gfile.GetBinContentByTwoOffsets(mtpf.File(), record.meta.start, record.meta.end); mtbuffer != nil {
                    record.meta.start -= int64(maxsize)
                    record.meta.cap   += maxsize
                    if _, err = mtpf.File().WriteAt(mtbuffer, record.meta.start); err == nil {
                        table.updateIndexByRecord(record)
                        table.checkAndResizeMtCap(record)
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
