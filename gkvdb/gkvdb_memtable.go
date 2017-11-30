package gkvdb

import "sync"

// 内存表,需要结合binlog一起使用
type MemTable struct {
    mu      sync.RWMutex            // 并发互斥锁
    db      *DB                     // 所属数据库
    datamap map[string]MemTableItem // 键名与事务对象指针的映射，便于通过键名直接查找事务对象，最新的操作会对老的键名进行覆盖
}

// 由于键值可能会很大，所以这里只存放键名和键值的地址
type MemTableItem struct{
    vlen  int   // 键值长度
    start int64 // 键值在binlog文件中的开始位置
}

// 创建一个MemTable
func newMemTable(db *DB) *MemTable {
    return &MemTable {
        db      : db,
        datamap : make(map[string]MemTableItem),
    }
}

// 保存事务
func (table *MemTable) set(tx *Transaction) error {
    table.mu.Lock()
    defer table.mu.Unlock()

    txid := int(tx.id)
    if _, ok := table.txidmap[txid]; !ok {
        table.txids         = append(table.txids, txid)
        table.txidmap[txid] = tx
    }
    for k, _ := range tx.datamap {
        table.txkvmap[k] = tx
    }
    return nil
}

// 查询键值对
func (table *MemTable) get(key []byte) ([]byte, bool) {
    table.mu.RLock()
    defer table.mu.RUnlock()

    if tx, ok := table.txkvmap[string(key)]; ok {
        value := tx.Get(key)
        if len(value) == 0 {
            return nil, true
        } else {
            return value, true
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
    if tx, ok := table.txidmap[txid]; ok {
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
    delete(table.txidmap, int(tx.id))
}

// 返回指定大小的键值对列表
func (table *MemTable) items(max int) map[string][]byte {
    table.mu.RLock()
    defer table.mu.RUnlock()
    m := make(map[string][]byte)
    for k, tx := range table.txkvmap {
        m[k] = tx.Get([]byte(k))
        if len(m) == max {
            break
        }
    }
    return m
}

// 同步缓存的binlog数据到底层数据库文件
func (table *MemTable) clear() {
    table.txids   = make([]int, 0)
    table.txidmap = make(map[int]*Transaction)
    table.txkvmap = make(map[string]*Transaction)
}