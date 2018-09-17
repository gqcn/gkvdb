// Copyright 2017 gkvdb Author(https://gitee.com/johng/gkvdb). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://gitee.com/johng/gkvdb.

// 数据结构要点   ：数据的分配长度cap >= 数据真实长度len，且 cap - len <= bucket，
//               当数据存储内容发生改变时，依靠碎片管理器对碎片进行回收再利用，且碎片大小 >= bucket

// 索引文件结构    ：元数据文件偏移量倍数(36bit,64GB*元数据桶大小)|下一层级索引的文件偏移量倍数(重复分区标志位=1时有效) 元数据文件列表项大小(19bit,524287)|分区增量 深度分区标识符(1bit)
// 元数据文件结构   :[键名哈希64(64bit) 键名长度(8bit) 键值长度(24bit,16MB) 数据文件偏移量(40bit,1TB)](变长)
// 数据文件结构    ：[键名长度(8bit) 键名 键值](变长)
// BinLog文件结构 ：注意binlog中的事务编号不是递增的，但是是唯一的
// [是否同步(8bit) 数据长度(32bit) 事务编号(64bit)] -- 事务开始
// [表名长度(8bit) 键名长度(8bit) 键值长度(24bit,16MB) 表名 键名 键值 ](变长，当键值长度为0表示删除)
// ...
// [事务编号(64bit)] -- 事务结束

// 基于DRH(Deep-Re-Hash)算法的高性能Key-Value嵌入式数据库.
package gkvdb

import (
    "sync"
    "strconv"
    "errors"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/encoding/ghash"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/container/gtype"
    "os"
)

const (
    gDEFAULT_PART_SIZE       = 100000                   // 默认哈希表分区大小
    gMAX_TABLE_SIZE          = 0xFF                     // 表名最大长度(255byte)
    gMAX_KEY_SIZE            = 0xFF                     // 键名最大长度(255byte)
    gMAX_VALUE_SIZE          = 0xFFFFFF                 // 键值最大长度(16MB)
    gMETA_ITEM_SIZE          = 17                       // 元数据单项大小(byte)
    gMAX_META_LIST_SIZE      = 65535*gMETA_ITEM_SIZE    // 阶数，元数据列表最大大小(byte)
    gMAX_DATA_FILE_SIZE      = 0xFFFFFFFFFF             // 数据文件最大大小(40bit, 1TB)
    gINDEX_BUCKET_SIZE       = 7                        // 索引文件数据块大小(byte)
    gMETA_BUCKET_SIZE        = 5*gMETA_ITEM_SIZE        // 元数据数据分块大小(byte, 值越大，数据增长时占用的空间越大)
    gDATA_BUCKET_SIZE        = 32                       // 数据分块大小(byte, 值越大，数据增长时占用的空间越大)
    gFILE_POOL_CACHE_TIMEOUT = 60000                    // 文件指针池缓存时间(秒)
    gCACHE_DEFAULT_TIMEOUT   = 10000                    // gcache默认缓存时间(毫秒)
    gAUTO_COMPACTING_MINSIZE = 512                      // 当空闲块大小>=该大小时，对其进行数据整理
    gAUTO_COMPACTING_TIMEOUT = 100                      // 自动进行数据整理的时间(毫秒)
    gBINLOG_MAX_SIZE         = 20*1024*1024             // binlog临时队列最大大小(byte)，超过该长度则强制性阻塞同步到数据文件
    gDEFAULT_TABLE_NAME      = "default"                // 默认的数据表名
)

// KV数据库
type DB struct {
    mu     sync.RWMutex             // API互斥锁
    path   string                   // 数据文件存放目录路径
    tables *gmap.StringInterfaceMap // 多表集合
    binlog *BinLog                  // BinLog
    closed *gtype.Bool              // 数据库是否关闭，以便异步线程进行判断处理
}

// 创建一个KV数据库，path指定数据库文件的存放目录绝对路径
func New(path string) (*DB, error) {
    db := &DB {
        path   : path,
        tables : gmap.NewStringInterfaceMap(),
        closed : gtype.NewBool(),
    }
    // 初始化数据库目录
    if !gfile.Exists(path) {
        gfile.Mkdir(path)
    }
    // 初始化BinLog
    if binlog, err := newBinLog(db); err != nil {
        return nil, err
    } else {
        db.binlog = binlog
    }

    // 自检并初始化相关服务
    db.binlog.initFromFile()
    go db.startAutoSyncingLoop()
    return db, nil
}

// 获取binlog文件绝对路径
func (db *DB) getBinLogFilePath() string {
    return db.path + gfile.Separator + "binlog"
}

// 获得binlog文件打开指针
func (db *DB) getBinlogFilePointer() (*os.File, error) {
    return os.OpenFile(db.getBinLogFilePath(), os.O_RDWR|os.O_CREATE, 0755)
}

// 关闭数据库链接，释放资源
func (db *DB) Close() {
    // 关闭数据库所有的表
    m := db.tables.Clone()
    for k, v := range m {
        table := v.(*Table)
        table.Close()
        db.tables.Remove(k)
    }
    // 关闭binlog
    db.binlog.close()
    // 设置关闭标识，使得异步线程自动关闭
    db.closed.Set(true)
}

// 计算关键字的hash code，使用64位哈希函数
func getHash64(key []byte) uint64 {
    return ghash.BKDRHash64(key)
}

// 根据元数据的size计算cap
func getMetaCapBySize(size int) int {
    if size > 0 && size%gMETA_BUCKET_SIZE != 0 {
        return size + gMETA_BUCKET_SIZE - size%gMETA_BUCKET_SIZE
    }
    return size
}

// 根据数据的size计算cap
func getDataCapBySize(size int) int {
    if size > 0 && size%gDATA_BUCKET_SIZE != 0 {
        return size + gDATA_BUCKET_SIZE - size%gDATA_BUCKET_SIZE
    }
    return size
}

// 检测键名合法性
func checkTableValid(name string) error {
    if len(name) > gMAX_TABLE_SIZE || len(name) == 0 {
        return errors.New("invalid table name size, should be in 1 and " + strconv.Itoa(gMAX_TABLE_SIZE) + " bytes")
    }
    return nil
}

// 检测键名合法性
func checkKeyValid(key []byte) error {
    if len(key) > gMAX_KEY_SIZE || len(key) == 0 {
        return errors.New("invalid key size, should be in 1 and " + strconv.Itoa(gMAX_KEY_SIZE) + " bytes")
    }
    return nil
}

// 检测键值合法性
func checkValueValid(value []byte) error {
    if len(value) > gMAX_VALUE_SIZE {
        return errors.New("too large value size, max allowed: " + strconv.Itoa(gMAX_VALUE_SIZE) + " bytes")
    }
    return nil
}


