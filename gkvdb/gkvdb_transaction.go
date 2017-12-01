package gkvdb

import (
    "sync"
    "gitee.com/johng/gf/g/util/gtime"
)

// 事务操作对象
type Transaction struct {
    mu        sync.RWMutex         // 并发互斥锁
    db        *DB                  // 所属数据库
    id        int64                // 事务编号
    datamap   map[string][]byte    // 事务内部的KV映射表，便于事务查询
}

// 创建一个事务
func (db *DB) Begin() *Transaction {
    return db.newTransaction()
}

// 创建一个事务对象
func (db *DB) newTransaction() *Transaction {
    tx := &Transaction {
        db      : db,
        id      : db.txid(),
        datamap : make(map[string][]byte),
    }
    return tx
}

// 生成一个唯一的事务编号
func (db *DB) txid() int64 {
    return gtime.Nanosecond()
}
// 添加数据
func (tx *Transaction) Set(key, value []byte) *Transaction {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    tx.datamap[string(key)] = value
    return tx
}

// 查询数据
func (tx *Transaction) Get(key []byte) []byte {
    tx.mu.RLock()
    defer tx.mu.RUnlock()

    if v, ok := tx.datamap[string(key)]; ok {
        return v
    }
    return tx.db.Get(key)
}

// 删除数据
func (tx *Transaction) Remove(key []byte) *Transaction {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    tx.datamap[string(key)] = nil
    return tx
}

// 提交数据
func (tx *Transaction) Commit() error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    if len(tx.datamap) == 0 {
        return nil
    }
    // 先写Binlog
    if err := tx.db.binlog.writeByTx(tx); err != nil {
        return err
    }
    // 再写内存表
    tx.db.memt.set(tx.datamap)
    // 重置事务
    tx.reset()
    return nil
}

// 回滚数据
func (tx *Transaction) Rollback() {
    tx.mu.Lock()
    defer tx.mu.Unlock()
    // 重置事务
    tx.reset()
}

// 重置事务(内部调用)
func (tx *Transaction) reset() {
    tx.id        = tx.db.txid()
    tx.datamap   = make(map[string][]byte)
}
