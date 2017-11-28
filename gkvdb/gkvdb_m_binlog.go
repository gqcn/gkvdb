package gkvdb

import (
    "gitee.com/johng/gf/g/encoding/gbinary"
    "gitee.com/johng/gf/g/os/gfile"
)

type BinLog struct {
    k []byte
    v []byte
}

// 从binlog文件中恢复未同步数据到memtable中
func (db *DB) initFromBinLog() {
    blbuffer := gfile.GetBinContents(db.getBinLogFilePath())
    if len(blbuffer) == 0 {
        return
    }

    for i := 0; i < len(blbuffer); {
        buffer    := blbuffer[i : i + 13]
        synced    := int(gbinary.DecodeToInt8(buffer[i  : i + 1]))
        blsize    := int(gbinary.DecodeToInt32(buffer[i + 1 : i + 5]))
        txidstart := gbinary.DecodeToInt64(buffer[i + 5 : i + 13])
        txidend   := gbinary.DecodeToInt64(blbuffer[i + 13 + blsize : i + 13 + blsize + 8])
        if txidstart == txidend {
            // 正常数据
            if synced != 1 {
                tx      := db.binlogBufferToTx(blbuffer[i + 13 : i + 13 + blsize])
                tx.start = int64(i)
            }
            i += i + 13 + blsize + 8
        } else {
            // 异常数据，需要矫正，查找到下一条正常的数据，中间的数据直接丢弃
            i += 13
        }
    }
}

// 将二进制数据转换为事务对象
func (db *DB) binlogBufferToTx(buffer []byte) *Transaction {
    tx := db.Begin()
    for i := 0; i < len(buffer); {
        bits  := gbinary.DecodeBytesToBits(buffer[i : i + 32])
        klen  := int(gbinary.DecodeBits(bits[i : i + 8]))
        vlen  := int(gbinary.DecodeBits(bits[i + 8 : i + 32]))
        key   := buffer[i + 32 : i + 32 + klen]
        value := buffer[i + 32 + klen : i + 32 + klen + vlen]
        tx.Set(key, value)
        i += i + 32 + klen + vlen
    }
    return tx
}

// 添加binlog到文件，支持批量添加
// 返回写入的文件开始位置，以及是否有错误
func (db *DB) addBinLog(txid int64, binlogs []*BinLog) (int64, error) {
    buffer := make([]byte, 0)
    // 数据列表
    for _, binlog := range binlogs {
        bits   := make([]gbinary.Bit, 0)
        bits    = gbinary.EncodeBits(bits, uint(len(binlog.k)),   8)
        bits    = gbinary.EncodeBits(bits, uint(len(binlog.v)),  24)
        buffer  = append(buffer, gbinary.EncodeBitsToBytes(bits)...)
        buffer  = append(buffer, binlog.k...)
        buffer  = append(buffer, binlog.v...)
    }
    // 事务开始
    buffer  = append(buffer, gbinary.EncodeInt8(0)...)
    buffer  = append(buffer, gbinary.EncodeInt32(int32(len(buffer)))...)
    buffer  = append(buffer, gbinary.EncodeInt64(int64(txid))...)
    // 事务结束
    buffer  = append(buffer, gbinary.EncodeInt64(txid)...)

    // 从指针池获取
    blpf, err := db.blfp.File()
    if err != nil {
        return -1, err
    }
    defer blpf.Close()
    // 写到文件末尾
    start, err := blpf.File().Seek(0, 2)
    if err != nil {
        return -1, err
    }
    // 执行数据写入
    if _, err := blpf.File().WriteAt(buffer, start); err != nil {
        return -1, err
    }
    return start, nil
}
