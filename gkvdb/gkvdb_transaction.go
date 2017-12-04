package gkvdb

import (
    "sync"
    "gitee.com/johng/gf/g/util/gtime"
)

// 事务操作对象
type Transaction struct {
    mu     sync.RWMutex                 // 并发互斥锁
    db     *DB                          // 所属数据库
    id     int64                        // 事务编号
    tables map[string]map[string][]byte // 事务数据项，键名为表名，键值为对应表的键值对数据
}

// 创建一个事务
func (db *DB) Begin() *Transaction {
    return db.newTransaction()
}

// 创建一个事务对象
func (db *DB) newTransaction() *Transaction {
    tx := &Transaction {
        db     : db,
        id     : db.txid(),
        tables : make(map[string]map[string][]byte),
    }
    return tx
}

// 生成一个唯一的事务编号
func (db *DB) txid() int64 {
    return gtime.Nanosecond()
}

// 添加数据
func (tx *Transaction) Set(key, value []byte) *Transaction {
    return tx.SetTo(key, value, gDEFAULT_TABLE_NAME)
}

// 添加数据(针对数据表)
func (tx *Transaction) SetTo(key, value []byte, name string) *Transaction {
    tx.mu.Lock()
    defer tx.mu.Unlock()
    if _, ok := tx.tables[name]; !ok {
        tx.tables[name] = make(map[string][]byte)
    }
    tx.tables[name][string(key)] = value
    return tx
}

// 查询数据
func (tx *Transaction) Get(key []byte) []byte {
    return tx.GetFrom(key, gDEFAULT_TABLE_NAME)
}

// 查询数据(针对数据表)
func (tx *Transaction) GetFrom(key []byte, table string) []byte {
    tx.mu.RLock()
    defer tx.mu.RUnlock()

    if _, ok := tx.tables[table]; ok {
        return tx.tables[table][string(key)]
    }
    return tx.db.Get(key)
}

// 删除数据
func (tx *Transaction) Remove(key []byte) *Transaction {
    return tx.RemoveFrom(key, gDEFAULT_TABLE_NAME)
}

// 删除数据(针对数据表)
func (tx *Transaction) RemoveFrom(key []byte, table string) *Transaction {
    return tx.SetTo(key, nil, table)
}

// 提交数据
func (tx *Transaction) Commit() error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    if len(tx.tables) == 0 {
        return nil
    }
    // 写Binlog
    if err := tx.db.binlog.writeByTx(tx); err != nil {
        return err
    }

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
    tx.id     = tx.db.txid()
    tx.tables = make(map[string]map[string][]byte)
}
