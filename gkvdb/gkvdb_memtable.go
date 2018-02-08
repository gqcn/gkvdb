package gkvdb

import (
    "sync"
)

// 内存表，保存临时的binlog数据
type MemTable struct {
    mu      sync.RWMutex            // 并发互斥锁
    table   *Table                  // 所属数据表
    datamap map[string][]byte       // 键名与事务对象指针的映射，便于通过键名直接查找事务对象，最新的操作会对老的键名进行覆盖
}

// 创建一个MemTable
func (table *Table) newMemTable() *MemTable {
    return &MemTable {
        table   : table,
        datamap : make(map[string][]byte),
    }
}

// 保存事务
func (mtable *MemTable) set(datamap map[string][]byte) {
    mtable.mu.Lock()
    defer mtable.mu.Unlock()

    for k, v := range datamap {
        mtable.datamap[k] = v
    }
}

// 查询键值对
func (mtable *MemTable) get(key []byte) ([]byte, bool) {
    mtable.mu.RLock()
    defer mtable.mu.RUnlock()

    if v, ok := mtable.datamap[string(key)]; ok {
        if v == nil {
            return nil, true
        } else {
            return v, true
        }
    }
    return nil, false
}

// 返回指定大小的键值对列表
func (mtable *MemTable) items(max int) map[string][]byte {
    mtable.mu.RLock()
    defer mtable.mu.RUnlock()

    m := make(map[string][]byte)
    for k, v := range mtable.datamap {
        if v != nil {
            m[k] = v
            if len(m) == max {
                break
            }
        }
    }
    return m
}

// 同步缓存的binlog数据到底层数据库文件
func (mtable *MemTable) clear() {
    mtable.datamap = make(map[string][]byte)
}