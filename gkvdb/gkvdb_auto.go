package gkvdb

import (
    "os"
    "time"
    "errors"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/encoding/gbinary"
    "gitee.com/johng/gf/g/os/gmlock"
)

// 数据文件自动整理
func (table *Table) startAutoCompactingLoop() {
    func() {
        for !table.closed.Val() {
            if err := table.autoCompactingData(); err != nil {
                glog.Error("data compacting error:", err)
                time.Sleep(time.Second)
            }
            if err := table.autoCompactingMeta(); err != nil {
                glog.Error("meta compacting error:", err)
                time.Sleep(time.Second)
            }
            time.Sleep(gAUTO_COMPACTING_TIMEOUT*time.Millisecond)
        }
    }()
}

// 开启自动同步线程
func (db *DB) startAutoSyncingLoop() {
    func() {
        for {
            select {
                case <- db.binlog.syncEvents:
                    db.binlog.sync()
                case <- db.binlog.closeEvents:
                    return
            }
        }
    }()
}


// 数据，将最大的空闲块依次往后挪，直到文件末尾，然后truncate文件
func (table *Table) autoCompactingData() error {
    key := "auto_compacting_data_cache_key_for_" + table.db.path + table.name
    if !gmlock.TryLock(key, 10000) {
        return nil
    }
    defer gmlock.Unlock(key)

    table.mu.Lock()
    defer table.mu.Unlock()

    maxsize := table.getDbFileSpaceMaxSize()
    if maxsize < gAUTO_COMPACTING_MINSIZE {
        return nil
    }
    index := table.getDbFileSpace(maxsize)
    if index < 0 {
        return nil
    }

    // 返回参数
    var retmsg error = nil
    dbpath  := table.getDataFilePath()
    dbsize  := gfile.Size(dbpath)
    dbstart := index + int64(maxsize)
    if dbstart == dbsize {
        // 如果碎片正好在文件末尾,那么直接truncate
        return os.Truncate(dbpath, int64(index))
    } else {
        if dbpf, retmsg := table.getDataFilePointer(); retmsg == nil {
            defer dbpf.Close()
            // 为防止截止位置超出文件长度，这里先获取键名长度
            if buffer := gfile.GetBinContentsByTwoOffsets(dbpf, dbstart, dbstart + 1); buffer != nil {
                klen   := gbinary.DecodeToUint8(buffer)
                key    := gfile.GetBinContentsByTwoOffsets(dbpf, dbstart + 1, dbstart + 1 + int64(klen))
                record := &_Record {
                    hash64  : uint(getHash64(key)),
                    key     : key,
                }
                // 查找对应数据的索引信息，并执行更新
                if retmsg = table.getIndexInfoByRecord(record); retmsg == nil {
                    if record.meta.end > 0 {
                        if retmsg = table.getDataInfoByRecord(record); retmsg == nil {
                            if dbbuffer := gfile.GetBinContentsByTwoOffsets(dbpf, record.data.start, record.data.end); dbbuffer != nil {
                                record.data.start -= int64(maxsize)
                                record.data.end   -= int64(maxsize)
                                if _, retmsg = dbpf.WriteAt(dbbuffer, record.data.start); retmsg == nil {
                                    // 保存查询记录对象，以便处理碎片
                                    orecord := *record
                                    // 更新已迁移的元数据信息
                                    if retmsg = table.saveMetaByRecord(record); retmsg == nil {
                                        // 更新已迁移的索引信息
                                        if retmsg = table.saveIndexByRecord(record); retmsg == nil {
                                            // 数据迁移成功之后再将碎片空间往后挪
                                            table.addDbFileSpace(int(record.data.start) + record.data.cap, maxsize)
                                            // 数据写入操作执行成功之后，才将旧数据添加进入碎片管理器
                                            table.addMtFileSpace(int(orecord.meta.start), orecord.meta.cap)
                                        }
                                    }
                                }
                            } else {
                                retmsg = errors.New("get data buffer nil")
                            }
                        }
                    } else {
                        // 如果找不到对应的索引信息，那么表示该数据区块为碎片
                        table.addDbFileSpace(int(record.data.start), record.data.cap)
                        retmsg = errors.New("invalid data buffer: meta info not found")
                    }
                }
            } else {
                retmsg = errors.New("get next data buffer nil")
            }
        }
    }
    // 如果执行失败，那么将碎片重新添加进入碎片管理器
    if retmsg != nil {
        table.addDbFileSpace(int(index), maxsize)
    }
    return retmsg
}

// 元数据，将最大的空闲块依次往后挪，直到文件末尾，然后truncate文件
func (table *Table) autoCompactingMeta() error {
    key := "auto_compacting_meta_cache_key_for_" + table.db.path + table.name
    if !gmlock.TryLock(key, 10000) {
        return nil
    }
    defer gmlock.Unlock(key)

    table.mu.Lock()
    defer table.mu.Unlock()

    maxsize := table.getMtFileSpaceMaxSize()
    if maxsize < gAUTO_COMPACTING_MINSIZE {
        return nil
    }
    index := table.getMtFileSpace(maxsize)
    if index < 0 {
        return nil
    }

    // 返回参数
    var retmsg error = nil
    mtsize  := gfile.Size(table.getMetaFilePath())
    mtstart := index + int64(maxsize)
    if mtstart == mtsize {
        if err := os.Truncate(table.getMetaFilePath(), int64(index)); err == nil {
            if index == 0 {
                // 如果所有meta已被清空，那么重新初始化索引文件
                gfile.PutBinContents(table.getIndexFilePath(), make([]byte, gINDEX_BUCKET_SIZE*gDEFAULT_PART_SIZE))
            }
            return nil
        } else {
            return err
        }
    } else {
        if mtpf, retmsg := table.getMetaFilePointer(); retmsg == nil {
            defer mtpf.Close()
            // 找到对应空闲块下一条meta item数据
            if buffer := gfile.GetBinContentsByTwoOffsets(mtpf, mtstart, mtstart + gMETA_ITEM_SIZE); buffer != nil {
                bits   := gbinary.DecodeBytesToBits(buffer)
                hash64 := gbinary.DecodeBitsToUint(bits[0 : 64])
                record := &_Record {
                    hash64  : hash64,
                }
                // 查找对应的索引信息，并执行更新
                if retmsg = table.getIndexInfoByRecord(record); retmsg == nil {
                    if mtbuffer := gfile.GetBinContentsByTwoOffsets(mtpf, record.meta.start, record.meta.end); mtbuffer != nil {
                        record.meta.start -= int64(maxsize)
                        record.meta.end   -= int64(maxsize)
                        if _, retmsg = mtpf.WriteAt(mtbuffer, record.meta.start); retmsg == nil {
                            // 更新已迁移的索引信息，随后不用处理旧有位置的元数据内容，更新索引成功之后旧有位置将被标识为碎片
                            if retmsg = table.saveIndexByRecord(record); retmsg == nil {
                                // 元数据迁移成功之后再将碎片空间往后挪
                                table.addMtFileSpace(int(record.meta.start) + record.meta.cap, maxsize)
                            }
                        }
                    } else {
                        retmsg = errors.New("get mtbuffer nil")
                    }
                }
            } else {
                retmsg = errors.New("get next meta buffer nil")
            }
        }
    }

    // 如果执行失败，那么将碎片重新添加进入碎片管理器
    if retmsg != nil {
        table.addMtFileSpace(int(index), maxsize)
    }
    return retmsg
}
