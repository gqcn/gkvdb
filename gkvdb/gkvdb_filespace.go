package gkvdb

import (
    "g/os/gfile"
    "g/encoding/gbinary"
    "g/os/gfilespace"
    "g/os/gcache"
)

// 初始化碎片管理器
func (db *DB) initFileSpace() {
    db.mtsp = gfilespace.New()
    db.dbsp = gfilespace.New()
}

func (db *DB) setFileSpaceDirty(dirty bool) {
    gcache.Set("filespace_dirty", dirty, 0)
}

func (db *DB) isFileSpaceDirty() bool {
    if v := gcache.Get("filespace_dirty"); v != nil {
        return v.(bool)
    }
    return false
}


// 元数据碎片
func (db *DB) addMtFileSpace(index int, size uint) {
    defer db.setFileSpaceDirty(true)
    db.mtsp.AddBlock(index, size)

}

func (db *DB) getMtFileSpace(size uint) int64 {
    defer db.setFileSpaceDirty(true)
    i, s := db.mtsp.GetBlock(size)
    if i >= 0 {
        extra := int(s - size)
        if extra > 0 {
            db.addMtFileSpace(i + int(size), uint(extra))
        }
        return int64(i)
    } else {
        pf, err := db.mtfp.File()
        if err != nil {
            return -1
        }
        defer pf.Close()

        start, err := pf.File().Seek(0, 2)
        if err != nil {
            return -1
        }
        return start
    }
    return -1
}

// 数据碎片
func (db *DB) addDbFileSpace(index int, size uint) {
    defer db.setFileSpaceDirty(true)
    db.dbsp.AddBlock(index, size)
}

func (db *DB) getDbFileSpace(size uint) int64 {
    defer db.setFileSpaceDirty(true)
    i, s := db.dbsp.GetBlock(size)
    if i >= 0 {
        extra := int(s - size)
        if extra > 0 {
            db.addDbFileSpace(i + int(size), uint(extra))
        }
        return int64(i)
    } else {
        pf, err := db.dbfp.File()
        if err != nil {
            return -1
        }
        defer pf.Close()

        start, err := pf.File().Seek(0, 2)
        if err != nil {
            return -1
        }
        return start
    }
    return -1
}

// 保存碎片数据到文件
func (db *DB) saveFileSpace() error {
    if !db.isFileSpaceDirty() {
        return nil
    }
    defer db.setFileSpaceDirty(false)
    mtbuffer := db.mtsp.Export()
    dbbuffer := db.dbsp.Export()
    if len(mtbuffer) > 0 || len(dbbuffer) > 0 {
        buffer   := make([]byte, 0)
        buffer    = append(buffer, gbinary.EncodeUint32(uint32(len(mtbuffer)))...)
        buffer    = append(buffer, gbinary.EncodeUint32(uint32(len(dbbuffer)))...)
        buffer    = append(buffer, mtbuffer...)
        buffer    = append(buffer, dbbuffer...)
        return gfile.PutBinContents(db.getSpaceFilePath(), buffer)
    }
    return nil
}

// 恢复碎片文件到内存
func (db *DB) restoreFileSpace() {
    buffer := gfile.GetBinContents(db.getSpaceFilePath())
    if len(buffer) > 8 {
        mtsize := gbinary.DecodeToUint32(buffer[0 : 4])
        dbsize := gbinary.DecodeToUint32(buffer[4 : 8])
        if mtsize > 0 {
            db.mtsp.Import(buffer[8 : 8 + mtsize])
        }
        if dbsize > 0 {
            db.mtsp.Import(buffer[8 + mtsize : 8 + mtsize + dbsize])
        }
    }
}