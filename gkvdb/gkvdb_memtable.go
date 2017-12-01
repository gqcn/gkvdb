package gkvdb

import "sync"

// 内存表，保存临时的binlog数据
type MemTable struct {
    mu      sync.RWMutex            // 并发互斥锁
    db      *DB                     // 所属数据库
    datamap map[string][]byte       // 键名与事务对象指针的映射，便于通过键名直接查找事务对象，最新的操作会对老的键名进行覆盖
}

// 创建一个MemTable
func newMemTable(db *DB) *MemTable {
    return &MemTable {
        db      : db,
        datamap : make(map[string][]byte),
    }
}

// 保存事务
func (table *MemTable) set(datamap map[string][]byte) error {
    table.mu.Lock()
    defer table.mu.Unlock()

    for k, v := range datamap {
        table.datamap[k] = v
    }
    return nil
}

// 查询键值对
func (table *MemTable) get(key []byte) ([]byte, bool) {
    table.mu.RLock()
    defer table.mu.RUnlock()

    if v, ok := table.datamap[string(key)]; ok {
        if len(v) == 0 {
            return nil, true
        } else {
            return v, true
        }
    }
    return nil, false
}

// 返回指定大小的键值对列表
func (table *MemTable) items(max int) map[string][]byte {
    table.mu.RLock()
    defer table.mu.RUnlock()

    if len(table.datamap) <= max {
        return table.datamap
    }
    m := make(map[string][]byte)
    for k, v := range table.datamap {
        m[k] = v
        if len(m) == max {
            break
        }
    }
    return m
}

// 同步缓存的binlog数据到底层数据库文件
func (table *MemTable) clear() {
    table.datamap = make(map[string][]byte)
}