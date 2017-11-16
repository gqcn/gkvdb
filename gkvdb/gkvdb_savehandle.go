package gkvdb

import "time"

// 自动保存线程循环
func (db *DB) startAutoSavingLoop() {
    go db.autoSavingDataLoop()
    go db.autoSavingSpaceLoop()
}

// 数据
func (db *DB) autoSavingDataLoop() {
    for !db.isClosed() {
        db.memt.sync()
        time.Sleep(gAUTO_SAVING_TIMEOUT*time.Millisecond)
    }
}

// 碎片
func (db *DB) autoSavingSpaceLoop() {
    for !db.isClosed() {
        db.saveFileSpace()
        time.Sleep(gAUTO_SAVING_TIMEOUT*time.Millisecond)
    }
}