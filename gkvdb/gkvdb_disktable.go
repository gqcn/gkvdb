package gkvdb

import (
    "g/os/gcache"
    "errors"
    "g/os/gfile"
    "g/encoding/gbinary"
    "bytes"
)


// 查询
func (db *DB) get(key []byte) []byte {
    ckey := "value_cache_" + string(key)
    if v := gcache.Get(ckey); v != nil {
        return v.([]byte)
    }
    db.mu.RLock()
    defer db.mu.RUnlock()

    value, _ := db.getValueByKey(key)
    gcache.Set(ckey, value, gCACHE_DEFAULT_TIMEOUT)
    return value
}

// 保存
func (db *DB) set(key []byte, value []byte) error {
    defer gcache.Remove("value_cache_" + string(key))

    db.mu.Lock()
    defer db.mu.Unlock()

    // 查询索引信息
    record, err := db.getRecordByKey(key)
    if err != nil {
        return err
    }

    // 值未改变不用重写
    if record.value != nil && bytes.Compare(value, record.value) == 0 {
        return nil
    }

    // 写入数据文件，并更新record信息
    if err := db.insertDataByRecord(key, value, record); err != nil {
        return errors.New("inserting data error: " + err.Error())
    }

    return nil
}


// 删除
func (db *DB) remove(key []byte) error {
    defer gcache.Remove("value_cache_" + string(key))

    db.mu.Lock()
    defer db.mu.Unlock()

    // 查询索引信息
    record, err := db.getRecordByKey(key)
    if err != nil {
        return err
    }
    // 如果找到匹配才执行删除操作
    if record.meta.match == 0 {
        return db.removeDataByRecord(record)
    }
    return nil
}

// 遍历
func (db *DB) items(max int) map[string][]byte {
    db.mu.RLock()
    defer db.mu.RUnlock()

    mtpf, err := db.mtfp.File()
    if err != nil {
        return nil
    }
    defer mtpf.Close()

    dbpf, err := db.dbfp.File()
    if err != nil {
        return nil
    }
    defer dbpf.Close()

    m := make(map[string][]byte)
    for start := int64(0); ; start += gMETA_BUCKET_SIZE {
        // 如果包含在碎片中，那么忽略
        if db.mtsp.Contains(int(start), gMETA_BUCKET_SIZE) {
            continue
        }
        if b := gfile.GetBinContentByTwoOffsets(mtpf.File(), start, start + gMETA_BUCKET_SIZE); b != nil {
            for i := 0; i < gMETA_BUCKET_SIZE; i += gMETA_ITEM_SIZE {
                if db.mtsp.Contains(i, gMETA_ITEM_SIZE) {
                    continue
                }
                buffer := b[i : i + gMETA_ITEM_SIZE]
                bits   := gbinary.DecodeBytesToBits(buffer)
                klen   := int(gbinary.DecodeBits(bits[64 : 72]))
                vlen   := int(gbinary.DecodeBits(bits[72 : 96]))
                if klen > 0 && vlen > 0 {
                    start := int64(gbinary.DecodeBits(bits[96 : 136]))*gDATA_BUCKET_SIZE
                    end   := start + int64(klen + vlen)
                    data  := gfile.GetBinContentByTwoOffsets(dbpf.File(), start, end)
                    key   := data[0 : klen]
                    value := data[klen :  ]
                    m[string(key)] = value
                    if len(m) == max {
                        return m
                    }
                }
            }
        } else {
            break
        }
    }
    return m
}
