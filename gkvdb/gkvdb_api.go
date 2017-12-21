package gkvdb

// =================================================================================
// 数据库操作
// =================================================================================

// 保存数据(默认表)
func (db *DB) Set(key []byte, value []byte) error {
    return db.SetTo(key, value, gDEFAULT_TABLE_NAME)
}

// 保存数据(数据表)
func (db *DB) SetTo(key []byte, value []byte, name string) error {
    tx := db.Begin()
    if err := tx.SetTo(key, value, name); err != nil {
        return err
    }
    return tx.Commit()
}

// 查询数据(默认表)
func (db *DB) Get(key []byte) []byte {
    return db.GetFrom(key, gDEFAULT_TABLE_NAME)
}

// 查询数据(数据表)
func (db *DB) GetFrom(key []byte, name string) []byte {
    if table, _ := db.Table(name); table != nil {
        return table.Get(key)
    }
    return nil
}

// 删除数据(默认表)
func (db *DB) Remove(key []byte) error {
    return db.RemoveFrom(key, gDEFAULT_TABLE_NAME)
}

// 删除数据(数据表)
func (db *DB) RemoveFrom(key []byte, name string) error {
    tx := db.Begin()
    if err := tx.RemoveFrom(key, name); err != nil {
        return err
    }
    return tx.Commit()
}

// 获取max条随机键值对，max=-1时获取所有数据返回
// 该方法会强制性遍历整个数据库
func (db *DB) Items(max int) map[string][]byte {
    table, _ := db.Table(gDEFAULT_TABLE_NAME)
    return table.Items(max)
}

// 获取最多max个随机键名，构成列表返回
func (db *DB) Keys(max int) []string {
    table, _ := db.Table(gDEFAULT_TABLE_NAME)
    return table.Keys(max)
}

// 获取最多max个随机键值，构成列表返回
func (db *DB) Values(max int) [][]byte {
    table, _ := db.Table(gDEFAULT_TABLE_NAME)
    return table.Values(max)
}

// =================================================================================
// 数据表操作
// =================================================================================

// 保存数据(数据表)
func (table *Table) Set(key []byte, value []byte) error {
    tx := table.db.Begin()
    if err := tx.SetTo(key, value, table.name); err != nil {
        return err
    }
    return tx.Commit()
}

// 查询数据(数据表)
func (table *Table) Get(key []byte) []byte {
    if v, ok := table.memt.get(key); ok {
        return v
    }
    return table.get(key)
}

// 删除数据(数据表)
func (table *Table) Remove(key []byte) error {
    tx := table.db.Begin()
    if err := tx.RemoveFrom(key, table.name); err != nil {
        return err
    }
    return tx.Commit()
}

// 随机遍历数据表
func (table *Table) Items(max int) map[string][]byte {
    // 先查询内存表
    m := table.memt.items(max)
    if max == -1 || max - len(m) > 0 {
        // 数据不够再遍历磁盘
        return table.items(max - len(m), m)
    }
    return m
}

// 获取最多max个随机键名，构成列表返回
func (table *Table) Keys(max int) []string {
    m    := table.Items(max)
    keys := make([]string, 0)
    for k, _ := range m {
        keys = append(keys, k)
    }
    return keys
}

// 获取最多max个随机键值，构成列表返回
func (table *Table) Values(max int) [][]byte {
    m      := table.Items(max)
    values := make([][]byte, 0)
    for _, v := range m {
        values = append(values, v)
    }
    return values
}


