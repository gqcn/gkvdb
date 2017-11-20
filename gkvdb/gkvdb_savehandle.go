package gkvdb

import (
    "time"
    "g/os/gcache"
)

// 自动保存线程循环
func (db *DB) startAutoSavingLoop() {
    go db.autoSavingDataLoop()
    go db.autoSavingSpaceLoop()
}

// 数据
func (db *DB) autoSavingDataLoop() {
    for !db.isClosed() {
        if db.isCacheEnabled() {
            db.autoSyncMemtable()
        }
        time.Sleep(gAUTO_SAVING_TIMEOUT*time.Millisecond)
    }
}

// 碎片
func (db *DB) autoSavingSpaceLoop() {
    for !db.isClosed() {
        if db.isCacheEnabled() {
            db.autoSyncFileSpace()
        }
        time.Sleep(gAUTO_SAVING_TIMEOUT*time.Millisecond)
    }
}

// 同步数据到磁盘
func (db *DB) sync() {
    db.memt.sync()
    db.saveFileSpace()
}

func (db *DB) autoSyncMemtable() {
    key := "memtable_sync_cache_key_for_" + db.path + db.name
    if gcache.Get(key) != nil {
        return
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    db.memt.sync()
}

func (db *DB) autoSyncFileSpace() {
    key := "filespace_sync_cache_key_for_" + db.path + db.name
    if gcache.Get(key) != nil {
        return
    }
    gcache.Set(key, struct{}{}, 86400)
    defer gcache.Remove(key)

    db.saveFileSpace()
}


