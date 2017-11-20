// 异步goroutine模块
package gkvdb

import (
    "time"
    "g/os/gcache"
    "g/os/gfile"
)

// 自动保存线程循环
func (db *DB) startAutoSavingLoop() {
    go db.autoSavingDataLoop()
    go db.autoSavingSpaceLoop()
}

// 数据迁移处理
func (db *DB) startCompactingLoop() {
    go func() {
        for !db.isClosed() {
            mtsize   := gfile.Size(db.getMetaFilePath())
            mtspsize := db.mtsp.SumSize()
            if float32(int64(mtspsize)/mtsize) >= gAUTO_COMPACTING_PERCENT {
                db.compactMeta()
            }
            dbsize   := gfile.Size(db.getDataFilePath())
            dbspsize := db.dbsp.SumSize()
            if float32(int64(dbspsize)/dbsize) >= gAUTO_COMPACTING_PERCENT {
                db.compactData()
            }
            time.Sleep(gAUTO_COMPACTING_TIMEOUT*time.Millisecond)
        }
    }()
}

// 元数据
func (db *DB) compactMeta() {

}

// 数据
func (db *DB) compactData() {

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


