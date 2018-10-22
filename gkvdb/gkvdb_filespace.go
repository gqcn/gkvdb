package gkvdb

import (
    "fmt"
    "sync"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/encoding/gbinary"
    "gitee.com/johng/gkvdb/gkvdb/gfilespace"
)

// 初始化碎片管理器
func (table *Table) initFileSpace() {
    table.mtsp = gfilespace.New()
    table.dbsp = gfilespace.New()
    // 这里使用的是写锁定，锁定表的所有数据操作
    table.mu.Lock()
    // 异步进行碎片计算，计算完成后解除写锁定
    go table.recountFileSpace()
}

// 重新计算空间碎片信息
// 必须锁起来防止数据变动引起计算不准确从而影响数据正确性
// 碎片计算是按照 index、meta、data进行依次的关联查询，数据不完整则算作是碎片
func (table *Table) recountFileSpace() {
    defer table.mu.Unlock()

    mtpf, _ := table.getMetaFilePointer()
    defer mtpf.Close()

    dbpf, _ := table.getDataFilePointer()
    defer dbpf.Close()

    usedmtsp := gfilespace.New()
    useddbsp := gfilespace.New()
    ixbuffer := gfile.GetBinContents(table.getIndexFilePath())

    // 并发计算ix,mt,db文件的使用情况
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
                    mtsp.AddBlock(int(mtindex), getMetaCapBySize(mtsize))
                    // 获取数据列表
                    if mtbuffer := gfile.GetBinContentsByTwoOffsets(mtpf, mtindex, mtindex + int64(mtsize)); mtbuffer != nil {
                        for i := 0; i < len(mtbuffer); i += gMETA_ITEM_SIZE {
                            buffer  := mtbuffer[i : i + gMETA_ITEM_SIZE]
                            bits    := gbinary.DecodeBytesToBits(buffer)
                            klen    := int(gbinary.DecodeBits(bits[64 : 72]))
                            vlen    := int(gbinary.DecodeBits(bits[72 : 96]))
                            dbcap   := getDataCapBySize(klen + vlen + 1)
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

    // 根据文件使用情况计算文件空白空间，即元数据碎片
    start  := 0
    end, _ := mtpf.Seek(0, 2)
    for _, v := range usedmtsp.GetAllBlocks() {
        if v.Index() > start {
            table.mtsp.AddBlock(start, v.Index() - start)
        }
        start = v.Index() + v.Size()
    }
    if start < int(end) {
        table.mtsp.AddBlock(start, int(end) - start)
    }
    // 根据文件使用情况计算文件空白空间，即数据碎片
    start  = 0
    end, _ = dbpf.Seek(0, 2)
    for _, v := range useddbsp.GetAllBlocks() {
        if v.Index() > start {
            table.dbsp.AddBlock(start, v.Index() - start)
        }
        start = v.Index() + v.Size()
    }
    if start < int(end) {
        table.dbsp.AddBlock(start, int(end) - start)
    }

    //fmt.Println("used mtsp:", usedmtsp.GetAllBlocks())
    //fmt.Println("used dbsp:", useddbsp.GetAllBlocks())
    //
    //fmt.Println("mtsp:", table.mtsp.GetAllBlocks())
    //fmt.Println("dbsp:", table.dbsp.GetAllBlocks())
    //fmt.Println("time:", gtime.Microsecond() - t)
    //
    //os.Exit(1)
}

func (table *Table) getMtFileSpaceMaxSize() int {
    return table.mtsp.GetMaxSize()
}

func (table *Table) getDbFileSpaceMaxSize() int {
    return table.dbsp.GetMaxSize()
}

// 添加元数据碎片
func (table *Table) addMtFileSpace(index int, size int) {
    table.mtsp.AddBlock(index, size)
}

// 申请元数据存储空间
func (table *Table) getMtFileSpace(size int) int64 {
    i, s := table.mtsp.GetBlock(size)
    if i >= 0 {
        extra := int(s - size)
        if extra > 0 {
            table.mtsp.AddBlock(i + int(size), extra)
        }
        return int64(i)
    } else {
        pf, err := table.getMetaFilePointer()
        if err != nil {
            return -1
        }
        defer pf.Close()

        start, err := pf.Seek(0, 2)
        if err != nil {
            return -1
        }
        return start
    }
    return -1
}

// 添加数据碎片
func (table *Table) addDbFileSpace(index int, size int) {
    table.dbsp.AddBlock(index, size)
}

// 申请数据存储空间
func (table *Table) getDbFileSpace(size int) int64 {
    i, s := table.dbsp.GetBlock(size)
    if i >= 0 {
        extra := s - size
        if extra > 0 {
            table.dbsp.AddBlock(i + int(size), extra)
        }
        return int64(i)
    } else {
        pf, err := table.getDataFilePointer()
        if err != nil {
            return -1
        }
        defer pf.Close()

        start, err := pf.Seek(0, 2)
        if err != nil || (start + int64(size)) > gMAX_DATA_FILE_SIZE {
            return -1
        }
        return start
    }
    return -1
}

// For Test Only
func (table *Table) PrintAllFileSpaces() {
    table.mu.Lock()
    defer table.mu.Unlock()
    fmt.Println("mt:", table.mtsp.GetAllBlocks())
    fmt.Println("db:", table.dbsp.GetAllBlocks())
}