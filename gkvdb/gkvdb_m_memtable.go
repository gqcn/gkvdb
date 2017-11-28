package gkvdb

// 内存表,需要结合binlog一起使用
type MemTable struct {
    db     *DB                   // 所属数据库
    data   map[string][]byte     // 临时BinLog数据，便于直接检索KV数据
    txes   []int                 // 事务编号列表，便于从小到大检索事务数据
    txdata map[int]*Transaction  // 事务编号对应的BinLog列表
}

// 创建一个MemTable
func newMemTable(db *DB) *MemTable {
    return &MemTable {
        db     : db,
        data   : make(map[string][]byte),
        txes   : make([]int, 0),
        txdata : make(map[int]*Transaction),
    }
}

// 保存
func (table *MemTable) set(tx *Transaction) error {
    txid := int(tx.id)
    if _, ok := table.txdata[txid]; !ok {
        table.txdata[txid] = tx
        table.txes         = append(table.txes, txid)
    }
    for _, v := range tx.binlogs {
        table.data[string(v.k)] = v.v
    }
    return nil
}

// 获取
func (table *MemTable) get(key []byte) ([]byte, bool) {
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
    if len(table.txes) == 0 {
        return nil
    }
    txid := table.txes[0]
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
    table.txes = table.txes[1:]
    delete(table.txdata, int(tx.id))
    for _, v := range tx.binlogs {
        delete(table.data, string(v.k))
    }
}
