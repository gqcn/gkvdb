package gkvdb

import (
    "g/encoding/gbinary"
    "fmt"
)

// 对数据库对应元数据列表进行重复分区
func (db *DB) checkDeepRehash(record *Record) error {
    if record.meta.size < gMAX_META_LIST_SIZE {
        return nil
    }
    fmt.Println("need rehash for:", record)
    // 将旧空间添加进碎片管理
    db.addMtFileSpace(int(record.meta.start), record.meta.cap)

    // 计算分区增量，保证数据散列(不保证最优，至少保证不需要重复分区即可)
    inc  := gDEFAULT_PART_SIZE + 1
    pmap := make(map[int][]byte)
    done := true
    for {
        for i := 0; i < record.meta.size; i += 17 {
            buffer := record.meta.buffer[i : i + 17]
            bits   := gbinary.DecodeBytesToBits(buffer)
            hash64 := gbinary.DecodeBits(bits[0 : 64])
            part   := int(hash64%uint(inc))
            if _, ok := pmap[part]; !ok {
                pmap[part] = make([]byte, 0)
            }
            pmap[part] = append(pmap[part], buffer...)
            if len(pmap[part]) == gMAX_META_LIST_SIZE {
                done = false
                pmap = make(map[int][]byte)
                inc++
                break
            }
        }
        if done {
            break
        } else {
            done = true
        }
    }

    // 计算元数据大小以便分配空间
    mtsize := 0
    for _, v := range pmap {
        mtsize += db.getMetaCapBySize(len(v))
    }
    // 生成写入的索引数据及元数据
    mtstart  := db.getMtFileSpace(mtsize)
    tmpstart := mtstart
    mtbuffer := make([]byte, 0)
    ixbuffer := make([]byte, 0)
    for i := 0; i < inc; i ++ {
        part := i
        if v, ok := pmap[part]; ok {
            bits     := make([]gbinary.Bit, 0)
            bits      = gbinary.EncodeBits(bits, uint(tmpstart)/gMETA_BUCKET_SIZE,   32)
            bits      = gbinary.EncodeBits(bits, uint(len(v))/17,                    16)
            bits      = gbinary.EncodeBits(bits, 0,                                  16)
            mtcap    := db.getMetaCapBySize(len(v))
            tmpstart += int64(mtcap)
            ixbuffer  = append(ixbuffer, gbinary.EncodeBitsToBytes(bits)...)
            mtbuffer  = append(mtbuffer, v...)
            for j := 0; j < int(mtcap) - len(v); j++ {
                mtbuffer = append(mtbuffer, byte(0))
            }
        } else {
            ixbuffer = append(ixbuffer, make([]byte, gINDEX_BUCKET_SIZE)...)
        }
    }

    // 写入重新分区后的元数据信息
    mtpf, err := db.mtfp.File()
    if err != nil {
        return err
    }
    defer mtpf.Close()
    if _, err = mtpf.File().WriteAt(mtbuffer, mtstart); err != nil {
        return err
    }

    // 写入重新分区后的索引信息
    ixpf, err := db.ixfp.File()
    if err != nil {
        return err
    }
    defer ixpf.Close()
    ixstart, err := ixpf.File().Seek(0, 2)
    if err != nil {
        return err
    }
    ixpf.File().WriteAt(ixbuffer, ixstart)

    // 修改老的索引信息
    bits    := make([]gbinary.Bit, 0)
    bits     = gbinary.EncodeBits(bits, uint(ixstart)/gINDEX_BUCKET_SIZE,  32)
    bits     = gbinary.EncodeBits(bits, 0,                                 16)
    bits     = gbinary.EncodeBits(bits, uint(inc) - gDEFAULT_PART_SIZE,    16)
    if _, err = ixpf.File().WriteAt(gbinary.EncodeBitsToBytes(bits), record.index.start); err != nil {
        return err
    }

    return nil
}