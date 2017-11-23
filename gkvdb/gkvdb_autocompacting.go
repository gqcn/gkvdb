// 自动迁移数据,去掉无用空间的goroutine
package gkvdb

import (
    "g/os/gfile"
    "os"
    "g/encoding/gbinary"
    "g/os/gcache"
    "time"
)

// 数据迁移处理
func (db *DB) startAutoCompactingLoop() {
    go func() {
        for !db.isClosed() {
            db.autoCompactingData()
            db.autoCompactingMeta()
            time.Sleep(gAUTO_COMPACTING_TIMEOUT*time.Millisecond)
        }
    }()
}

// 数据，将最大的空闲块依次往后挪，直到文件末尾，然后truncate文件
func (db *DB) autoCompactingData() {
    key := "auto_compacting_data_cache_key_for_" + db.path + db.name
    if gcache.Get(key) != nil {
        return
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    db.mu.Lock()
    defer db.mu.Unlock()

    maxsize := db.getDbFileSpaceMaxSize()
    if maxsize < gAUTO_COMPACTING_MINSIZE {
        return
    }
    index := db.getDbFileSpace(maxsize)
    if index < 0 {
        return
    }
    dbsize  := gfile.Size(db.getDataFilePath())
    dbstart := index + int64(maxsize)
    if dbstart == dbsize {
        os.Truncate(db.getDataFilePath(), int64(index))
    } else {
        dbpf, err := db.dbfp.File()
        if err != nil {
            return
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
                    }
                }
            }
        }
    }
}

// 元数据，将最大的空闲块依次往后挪，直到文件末尾，然后truncate文件
func (db *DB) autoCompactingMeta() {
    key := "auto_compacting_meta_cache_key_for_" + db.path + db.name
    if gcache.Get(key) != nil {
        return
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    db.mu.Lock()
    defer db.mu.Unlock()

    maxsize := db.getMtFileSpaceMaxSize()
    if maxsize < gAUTO_COMPACTING_MINSIZE {
        return
    }
    index := db.getMtFileSpace(maxsize)
    if index < 0 {
        return
    }
    mtsize  := gfile.Size(db.getMetaFilePath())
    mtstart := index + int64(maxsize)
    if mtstart == mtsize {
        os.Truncate(db.getMetaFilePath(), int64(index))
    } else {
        mtpf, err := db.mtfp.File()
        if err != nil {
            return
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
                    }
                }
            }
        }
    }
}
