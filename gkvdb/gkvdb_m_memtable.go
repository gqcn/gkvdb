package gkvdb

import "sync"

// 内存表,需要结合binlog一起使用
type MemTable struct {
    mu     sync.RWMutex          // 并发互斥锁
    db     *DB                   // 所属数据库
    data   map[string][]byte     // 临时BinLog数据，便于直接检索KV数据
    txids  []int                 // 事务编号列表，便于从小到大检索事务数据
    txdata map[int]*Transaction  // 事务编号对应的BinLog列表
}

// 创建一个MemTable
func newMemTable(db *DB) *MemTable {
    return &MemTable {
        db     : db,
        data   : make(map[string][]byte),
        txids  : make([]int, 0),
        txdata : make(map[int]*Transaction),
    }
}

// 保存
func (table *MemTable) set(tx *Transaction) error {
    table.mu.Lock()
    defer table.mu.Unlock()

    txid := int(tx.id)
    if _, ok := table.txdata[txid]; !ok {
        table.txdata[txid] = tx
        table.txids        = append(table.txids, txid)
    }
    for _, v := range tx.binlogs {
        table.data[string(v.k)] = v.v
    }
    return nil
}

// 获取
func (table *MemTable) get(key []byte) ([]byte, bool) {
    table.mu.RLock()
    defer table.mu.RUnlock()

    if v, ok := table.data[string(key)]; ok {
        if len(v) == 0 {
            return nil, true
        } else {
            return v, true
        }
    }
    return nil, false
}

// 获取最小的Tx数据(不删除)
func (table *MemTable) getMinTx() *Transaction {
    table.mu.RLock()
    defer table.mu.RUnlock()

    if len(table.txids) == 0 {
        return nil
    }
    txid := table.txids[0]
    if tx, ok := table.txdata[txid]; ok {
        return tx
    }
    return nil
}

// 删除最小的Tx数据
func (table *MemTable) removeMinTx() {
    tx := table.getMinTx()
    if tx == nil {
        return
    }

    table.mu.Lock()
    defer table.mu.Unlock()

    table.txids = table.txids[1:]
    delete(table.txdata, int(tx.id))
    for _, v := range tx.binlogs {
        delete(table.data, string(v.k))
    }
}
