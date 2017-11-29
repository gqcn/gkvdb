// 缓存启用时的自动保存goroutine
package gkvdb

import (
    "os"
    "time"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gcache"
)

// 自动保存线程循环
func (db *DB) startAutoSavingLoop() {
    go func() {
        for !db.isClosed() {
            if err := db.doAutoSaving(); err != nil {
                glog.Error(err)
                time.Sleep(time.Minute)
            }
            time.Sleep(gAUTO_SAVING_TIMEOUT*time.Millisecond)
        }
    }()
}

// 自动保存binlog的数据到数据表中
func (db *DB) doAutoSaving() error {
    key := "auto_saving_cache_key_for_" + db.path + db.name
    if gcache.Get(key) != nil {
        return nil
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    for {
        if tx := db.memt.getMinTx(); tx != nil {
            if err := tx.sync(); err != nil {
                return err
            } else {
                if err := db.markTxSynced(tx); err == nil {
                    db.memt.removeMinTx()
                }
            }
        } else {
            // 如果所有的事务数据已经同步完成，那么矫正binblog文件大小
            os.Truncate(db.getBinLogFilePath(), 0)
            break
        }
    }
    return nil
}
