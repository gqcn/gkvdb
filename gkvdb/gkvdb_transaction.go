package gkvdb

import (
    "gitee.com/johng/gf/g/util/gtime"
    "sync"
    "errors"
)

// 事务操作对象
type Transaction struct {
    mu        sync.RWMutex          // 并发互斥锁
    db        *DB                   // 所属数据库
    id        int64                 // 事务编号
    start     int64                 // BinLog文件开始位置
    items     []*TransactionItem    // BinLog数据，保证写入顺序
    datamap   map[string][]byte     // 事务内部的KV映射表，便于事务查询
    committed bool                  // 事务是否已经提交到binlog文件
}

// 事务数据项
type TransactionItem struct {
    k []byte
    v []byte
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
        start   : -1,
        items   : make([]*TransactionItem, 0),
        datamap : make(map[string][]byte),
    }
    return tx
}

// 生成一个唯一的事务编号
func (db *DB) txid() int64 {
    return gtime.Nanosecond()
}

// 标识为已提交到binlog
func (tx *Transaction) setAsCommitted() {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    tx.committed = true
}

// 添加数据
func (tx *Transaction) Set(key, value []byte) *Transaction {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    if tx.committed {
        tx.reset()
    }
    tx.items                = append(tx.items, &TransactionItem{key, value})
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

    if tx.committed {
        tx.reset()
    }
    tx.items                = append(tx.items, &TransactionItem{key, nil})
    tx.datamap[string(key)] = nil
    return tx
}

// 提交数据
func (tx *Transaction) Commit() error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    if len(tx.items) == 0 || tx.committed {
        return nil
    }
    // 先写Binlog
    start, err := tx.db.addBinLogByTx(tx)
    if err != nil {
        return err
    }
    tx.start     = start
    tx.committed = true
    // 再写内存表，这里创建一个新的变量，内存表中保存的事务指针是该变量的指针
    tx.db.memt.set(tx.copy())
    return nil
}

// 回滚数据
func (tx *Transaction) Rollback() {
    tx.mu.Lock()
    defer tx.mu.Unlock()
    tx.reset()
}

// 创建一个事务的拷贝(数据拷贝)
func (tx *Transaction) copy() *Transaction {
    newtx          := tx.db.newTransaction()
    newtx.id        = tx.id
    newtx.start     = tx.start
    newtx.items     = tx.items
    newtx.datamap   = tx.datamap
    newtx.committed = tx.committed
    return newtx
}

// 重置事务(内部调用)
func (tx *Transaction) reset() {
    tx.id        = tx.db.txid()
    tx.start     = -1
    tx.items     = make([]*TransactionItem, 0)
    tx.datamap   = make(map[string][]byte)
    tx.committed = false
}

// 将事务事务同步到磁盘
// 注意，必须先要保证该数据已经commit到binlog文件中
func (tx *Transaction) sync() error {
    tx.mu.RLock()
    defer tx.mu.RUnlock()

    if !tx.committed {
        return errors.New("uncommitted transaction")
    }

    for _, item := range tx.items {
        if len(item.v) == 0 {
            if err := tx.db.remove(item.k); err != nil {
                return err
            }
        } else {
            if err := tx.db.set(item.k, item.v); err != nil {
                return err
            }
        }
    }
    return nil
}