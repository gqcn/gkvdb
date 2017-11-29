package gkvdb

import (
    "errors"
    "strconv"
)

// 查询KV数据
func (db *DB) Get(key []byte) []byte {
    if v, ok := db.memt.get(key); ok {
        return v
    }
    return db.get(key)
}

// 设置KV数据
func (db *DB) Set(key []byte, value []byte) error {
    if len(key) > gMAX_KEY_SIZE || len(key) == 0 {
        return errors.New("invalid key size, should be in 1 and " + strconv.Itoa(gMAX_KEY_SIZE) + " bytes")
    }
    if len(value) > gMAX_VALUE_SIZE {
        return errors.New("too large value size, max allowed: " + strconv.Itoa(gMAX_VALUE_SIZE) + " bytes")
    }
    return db.Begin().Set(key, value).Commit()
}

// 删除KV数据
func (db *DB) Remove(key []byte) error {
    if len(key) > gMAX_KEY_SIZE || len(key) == 0 {
        return errors.New("invalid key size, should be in 1 and " + strconv.Itoa(gMAX_KEY_SIZE) + " bytes")
    }
    return db.Begin().Remove(key).Commit()
}

// 获取max条随机键值对，max=-1时获取所有数据返回
// 该方法会强制性遍历整个数据库
func (db *DB) Items(max int) map[string][]byte {
    // 先查询内存表
    m := db.memt.items(max)
    if max == -1 || max - len(m) > 0 {
        // 数据不够再遍历磁盘
        return db.items(max - len(m), m)
    }
    return m
}

// 获取最多max个随机键名，构成列表返回
func (db *DB) Keys(max int) []string {
    m    := db.Items(max)
    keys := make([]string, 0)
    for k, _ := range m {
        keys = append(keys, k)
    }
    return keys
}

// 获取最多max个随机键值，构成列表返回
func (db *DB) Values(max int) [][]byte {
    m      := db.Items(max)
    values := make([][]byte, 0)
    for _, v := range m {
        values = append(values, v)
    }
    return values
}

// 关闭数据库链接
func (db *DB) Close() {
    db.close()
}

// 打印数据库状态(调试使用)
//func (db *DB) PrintState() {
//    mtblocks := db.mtsp.GetAllBlocks()
//    dbblocks := db.dbsp.GetAllBlocks()
//    fmt.Println("meta pieces:")
//    fmt.Println("       size:", len(mtblocks))
//    fmt.Println("       list:", mtblocks)
//
//    fmt.Println("data pieces:")
//    fmt.Println("       size:", len(dbblocks))
//    fmt.Println("       list:", dbblocks)
//
//    fmt.Println("=======================================")
//}


