package gkvdb

import (
    "sync"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/gfilespace"
    "gitee.com/johng/gf/g/encoding/gbinary"
)

// 初始化碎片管理器
func (db *DB) initFileSpace() {
    db.mtsp = gfilespace.New()
    db.dbsp = gfilespace.New()
    // 异步计算碎片详情
    go db.recountFileSpace()
}

// 重新计算空间碎片信息
// 必须锁起来防止数据变动引起计算不准确从而影响数据正确性
func (db *DB) recountFileSpace() {
    db.mu.Lock()
    defer db.mu.Unlock()

    mtpf, _ := db.mtfp.File()
    defer mtpf.Close()

    dbpf, _ := db.dbfp.File()
    defer dbpf.Close()

    usedmtsp := gfilespace.New()
    useddbsp := gfilespace.New()
    ixbuffer := gfile.GetBinContents(db.getIndexFilePath())

    // 并发计算碎片
    var wg sync.WaitGroup
    group := gINDEX_BUCKET_SIZE
    piece := len(ixbuffer)/group
    piece += group - piece%group
    for g := 0; g < group; g++ {
        ss := g*piece
        se := (g + 1)*piece
        if ss >= len(ixbuffer) {
            break
        }
        if se > len(ixbuffer) {
            se = len(ixbuffer)
        }
        wg.Add(1)
        go func(ixbuffer []byte) {
            mtsp := gfilespace.New()
            dbsp := gfilespace.New()
            for i := 0; i < len(ixbuffer); i += gINDEX_BUCKET_SIZE {
                bits := gbinary.DecodeBytesToBits(ixbuffer[i : i + gINDEX_BUCKET_SIZE])
                if gbinary.DecodeBits(bits[55 : 56]) != 0 {
                    continue
                }
                mtindex := int64(gbinary.DecodeBits(bits[0 : 36]))*gMETA_BUCKET_SIZE
                mtsize  := int(gbinary.DecodeBits(bits[36 : 55]))*gMETA_ITEM_SIZE
                if mtsize > 0 {
                    mtsp.AddBlock(int(mtindex), db.getMetaCapBySize(mtsize))
                    // 获取数据列表
                    if mtbuffer := gfile.GetBinContentByTwoOffsets(mtpf.File(), mtindex, mtindex + int64(mtsize)); mtbuffer != nil {
                        for i := 0; i < len(mtbuffer); i += gMETA_ITEM_SIZE {
                            buffer  := mtbuffer[i : i + gMETA_ITEM_SIZE]
                            bits    := gbinary.DecodeBytesToBits(buffer)
                            klen    := int(gbinary.DecodeBits(bits[64 : 72]))
                            vlen    := int(gbinary.DecodeBits(bits[72 : 96]))
                            dbcap   := db.getDataCapBySize(klen + vlen + 1)
                            dbindex := int64(gbinary.DecodeBits(bits[96 : 136]))*gDATA_BUCKET_SIZE
                            if dbcap > 0 {
                                dbsp.AddBlock(int(dbindex), dbcap)
                            }
                        }
                    }
                }
            }
            // 整合到外部变量中
            for _, v := range mtsp.GetAllBlocks() {
                usedmtsp.AddBlock(v.Index(), v.Size())
            }
            for _, v := range dbsp.GetAllBlocks() {
                useddbsp.AddBlock(v.Index(), v.Size())
            }
            //fmt.Println("done")
            wg.Done()
        }(ixbuffer[ss : se])
        //fmt.Println(ss, se)
    }
    wg.Wait()

    // 计算元数据碎片
    start  := 0
    end, _ := mtpf.File().Seek(0, 2)
    for _, v := range usedmtsp.GetAllBlocks() {
        if v.Index() > start {
            db.mtsp.AddBlock(start, v.Index() - start)
        }
        start = v.Index() + v.Size()
    }
    if start < int(end) {
        db.mtsp.AddBlock(start, int(end) - start)
    }
    // 计算数据碎片
    start  = 0
    end, _ = dbpf.File().Seek(0, 2)
    for _, v := range useddbsp.GetAllBlocks() {
        if v.Index() > start {
            db.dbsp.AddBlock(start, v.Index() - start)
        }
        start = v.Index() + v.Size()
    }
    if start < int(end) {
        db.dbsp.AddBlock(start, int(end) - start)
    }
    //fmt.Println("used mtsp:", len(usedmtsp.GetAllBlocks()))
    //fmt.Println("used dbsp:", len(useddbsp.GetAllBlocks()))

    //fmt.Println("mtsp:", len(db.mtsp.GetAllBlocks()))
    //fmt.Println("dbsp:", len(db.dbsp.GetAllBlocks()))
    //fmt.Println("time:", gtime.Microsecond() - t)
    //
    //os.Exit(1)
}

func (db *DB) getMtFileSpaceMaxSize() int {
    return db.mtsp.GetMaxSize()
}

func (db *DB) getDbFileSpaceMaxSize() int {
    return db.dbsp.GetMaxSize()
}

// 元数据碎片
func (db *DB) addMtFileSpace(index int, size int) {
    db.mtsp.AddBlock(index, size)
}

func (db *DB) getMtFileSpace(size int) int64 {
    i, s := db.mtsp.GetBlock(size)
    if i >= 0 {
        extra := int(s - size)
        if extra > 0 {
            db.mtsp.AddBlock(i + int(size), extra)
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
func (db *DB) addDbFileSpace(index int, size int) {
    db.dbsp.AddBlock(index, size)
}

func (db *DB) getDbFileSpace(size int) int64 {
    i, s := db.dbsp.GetBlock(size)
    if i >= 0 {
        extra := s - size
        if extra > 0 {
            db.dbsp.AddBlock(i + int(size), extra)
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