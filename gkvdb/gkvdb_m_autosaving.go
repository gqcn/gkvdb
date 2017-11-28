// 缓存启用时的自动保存goroutine
package gkvdb

import (
    "time"
    "gitee.com/johng/gf/g/os/gcache"
    "gitee.com/johng/gf/g/encoding/gbinary"
    "os"
)

// 自动保存线程循环
func (db *DB) startAutoSavingLoop() {
    go func() {
        for !db.isClosed() {
            db.autoSaving()
            time.Sleep(gAUTO_SAVING_TIMEOUT*time.Millisecond)
        }
    }()
}

// 自动保存binlog的数据到数据表中
func (db *DB) autoSaving() {
    key := "auto_saving_cache_key_for_" + db.path + db.name
    if gcache.Get(key) != nil {
        return
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    for {
        if tx := db.memt.getMinTx(); tx != nil {
            for _, binlog := range tx.binlogs {
                if len(binlog.v) == 0 {
                    db.remove(binlog.k)
                } else {
                    db.set(binlog.k, binlog.v)
                }
            }
            if err := db.markTxSynced(tx); err == nil {
                db.memt.removeMinTx()
            }
        } else {
            // 如果所有的事务数据已经同步完成，那么矫正binblog文件大小
            os.Truncate(db.getBinLogFilePath(), 0)
        }
    }
}

// 写入磁盘，标识事务已经同步，在对应位置只写入1个字节
func (db *DB) markTxSynced(tx *Transaction) error {
    blpf, err := db.blfp.File()
    if err != nil {
        return err
    }
    defer blpf.Close()
    if _, err := blpf.File().WriteAt(gbinary.EncodeInt8(1), tx.start); err != nil {
        return err
    }
    return nil
}
