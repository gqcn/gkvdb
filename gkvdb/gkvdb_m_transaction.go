package gkvdb

import "gitee.com/johng/gf/g/util/gtime"

// 事务操作对象
type Transaction struct {
    db      *DB       // 所属数据库
    id      int64     // 事务编号
    start   int64     // BinLog文件开始位置
    binlogs []*BinLog // BinLog数据
}

// 创建一个事务
func (db *DB) Begin() *Transaction {
    tx := &Transaction {
        db      : db,
        id      : gtime.Nanosecond(),
        start   : -1,
        binlogs : make([]*BinLog, 0),
    }
    return tx
}

// 添加数据
func (tx *Transaction) Set(key, value []byte) {
    tx.binlogs = append(tx.binlogs, &BinLog{key, value})
}

// 删除数据
func (tx *Transaction) Remove(key []byte) {
    tx.binlogs = append(tx.binlogs, &BinLog{key, nil})
}

// 提交数据
func (tx *Transaction) Commit() error {
    start, err := tx.db.addBinLog(tx.id, tx.binlogs)
    if err != nil {
        return err
    }
    tx.start = start
    return nil
}

// 回滚数据
func (tx *Transaction) Rollback() error {
    tx.start   = -1
    tx.binlogs = make([]*BinLog, 0)
    return nil
}