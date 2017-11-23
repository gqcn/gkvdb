// 缓存启用时的自动保存goroutine
package gkvdb

import (
    "time"
    "gitee.com/johng/gf/g/os/gcache"
)

// 自动保存线程循环
func (db *DB) startAutoSavingLoop() {
    go func() {
        for !db.isClosed() {
            if db.isCacheEnabled() {
                db.autoSyncMemtable()
            }
            time.Sleep(gAUTO_SAVING_TIMEOUT*time.Millisecond)
        }
    }()
}

func (db *DB) autoSyncMemtable() {
    key := "auto_sync_memtable_cache_key_for_" + db.path + db.name
    if gcache.Get(key) != nil {
        return
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    db.memt.sync()
}
