package gkvdb

import (
    "sync"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
)

// 事务操作对象
type Transaction struct {
    mu     sync.RWMutex                 // 并发互斥锁
    db     *DB                          // 所属数据库
    id     int64                        // 事务编号
    table  string                       // 事务默认表
    tables map[string]map[string][]byte // 事务数据项，键名为表名，键值为对应表的键值对数据
}

// 创建一个事务
func (db *DB) Begin(table...string) *Transaction {
    tx := db.newTransaction()
    if len(table) > 0 {
        tx.table = table[0]
    } else {
        tx.table = gDEFAULT_TABLE_NAME
    }
    return tx
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
func (tx *Transaction) Set(key, value []byte) error{
    return tx.SetTo(key, value, tx.table)
}

// 添加数据(针对数据表)
func (tx *Transaction) SetTo(key, value []byte, name string) error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    // 每一次操作都要执行表名、键名、键值长度检查
    if err := checkTableValid(name); err != nil {
        return err
    }
    if err := checkKeyValid(key); err != nil {
        return err
    }
    if err := checkValueValid(key); err != nil {
        return err
    }

    if _, ok := tx.tables[name]; !ok {
        tx.tables[name] = make(map[string][]byte)
    }
    if value != nil {
        tx.tables[name][string(key)] = make([]byte, len(value))
    }
    copy(tx.tables[name][string(key)], value)
    return nil
}

// 查询数据
func (tx *Transaction) Get(key []byte) []byte {
    return tx.GetFrom(key, gDEFAULT_TABLE_NAME)
}

// 查询数据(针对数据表)
func (tx *Transaction) GetFrom(key []byte, name string) []byte {
    tx.mu.RLock()
    defer tx.mu.RUnlock()
    if _, ok := tx.tables[name]; ok {
        return tx.tables[name][string(key)]
    }
    return tx.db.GetFrom(key, name)
}

// 删除数据
func (tx *Transaction) Remove(key []byte) error {
    return tx.RemoveFrom(key, gDEFAULT_TABLE_NAME)
}

// 删除数据(针对数据表)
func (tx *Transaction) RemoveFrom(key []byte, name string) error {
    return tx.SetTo(key, nil, name)
}

// 提交数据
func (tx *Transaction) Commit(sync...bool) error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    if len(tx.tables) == 0 {
        return nil
    }
    // 写Binlog
    if err := tx.db.binlog.writeByTx(tx, sync...); err != nil {
        glog.Error(err)
        return err
    }

    // 重置事务
    tx.reset()
    return nil
}

// 回滚数据
func (tx *Transaction) Rollback() {
    tx.mu.Lock()
    // 重置事务
    tx.reset()
    tx.mu.Unlock()
}

// 重置事务(内部调用)
func (tx *Transaction) reset() {
    tx.id     = tx.db.txid()
    tx.tables = make(map[string]map[string][]byte)
}
