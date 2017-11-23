package main

import (
    "fmt"
    "gitee.com/johng/gf/g/util/gtime"
    "strconv"
    "github.com/boltdb/bolt"
    "log"
)

var db *bolt.DB

func init() {
    t := gtime.Microsecond()
    db, _    = bolt.Open("/tmp/boltdb", 0600, nil)
    tx, err := db.Begin(true)
    if err != nil {
        log.Fatal(err)
    }
    defer tx.Commit()
    //if _, err := tx.CreateBucket([]byte("test")); err != nil {
    //    log.Fatal(err)
    //}
    fmt.Println("db init:", gtime.Microsecond() - t)
}

// 测试默认带缓存情况下的数据库写入
func TestSetWithCache(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key   := []byte("key_" + strconv.Itoa(i))
        value := []byte("value_" + strconv.Itoa(i))
        db.Update(func(tx *bolt.Tx) error {
            b   := tx.Bucket([]byte("test"))
            err := b.Put(key, value)
            return err
        })
    }
    fmt.Println("TestSetWithCache:", gtime.Microsecond() - t)
}

// 测试默认带缓存情况下的数据库查询
func TestGetWithCache(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key := []byte("key_" + strconv.Itoa(i))
        db.View(func(tx *bolt.Tx) error {
            b := tx.Bucket([]byte("test"))
            if r := b.Get(key); r == nil {
                fmt.Println("TestGetWithoutCache value not found for index:", i)
            }
            return nil
        })
    }
    fmt.Println("TestGetWithCache:", gtime.Microsecond() - t)
}

// 测试默认带缓存情况下的数据库删除
func TestRemoveWithCache(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key := []byte("key_" + strconv.Itoa(i))
        db.Update(func(tx *bolt.Tx) error {
            b := tx.Bucket([]byte("test"))
            if err := b.Delete(key); err != nil {
                fmt.Println(err)
            }
            return nil
        })
    }
    fmt.Println("TestRemoveWithCache:", gtime.Microsecond() - t)
}



//// 测试不带缓存情况下的数据库写入
//func TestSetWithoutCache(count int) {
//    t := gtime.Microsecond()
//    for i := 0; i < count; i++ {
//        key   := []byte("key_" + strconv.Itoa(i))
//        value := []byte("value_" + strconv.Itoa(i))
//        if err := db.Put(key, value, &opt.WriteOptions{Sync:true}); err != nil {
//            fmt.Println(err)
//        }
//    }
//    fmt.Println("TestSetWithoutCache:", gtime.Microsecond() - t)
//}
//
//// 测试不带缓存情况下的数据库查询
//func TestGetWithoutCache(count int) {
//    t := gtime.Microsecond()
//    for i := 0; i < count; i++ {
//        key := []byte("key_" + strconv.Itoa(i))
//        if r, _ := db.Get(key, nil); r == nil {
//            fmt.Println("TestGetWithoutCache value not found for index:", i)
//        }
//    }
//    fmt.Println("TestGetWithoutCache:", gtime.Microsecond() - t)
//}
//
//// 测试不带缓存情况下的数据库删除
//func TestRemoveWithoutCache(count int) {
//    t := gtime.Microsecond()
//    for i := 0; i < count; i++ {
//        key := []byte("key_" + strconv.Itoa(i))
//        if err := db.Delete(key, &opt.WriteOptions{Sync:true}); err != nil {
//            fmt.Println(err)
//        }
//    }
//    fmt.Println("TestRemoveWithoutCache:", gtime.Microsecond() - t)
//}

func main() {
    var count int = 0

    //
    // ==================不带缓存的KV操作=======================
    //count = 10000
    //TestSetWithoutCache(count)
    //TestGetWithoutCache(count)
    //TestRemoveWithoutCache(count)

    // ==================带缓存的KV操作=======================
    // 100W性能测试
    count  = 10000
    //TestSetWithCache(count)
    TestGetWithCache(count)
    //TestRemoveWithCache(count)

}