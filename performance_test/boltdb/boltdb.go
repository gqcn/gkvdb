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
func TestSet(count int) {
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
    fmt.Println("TestSet:", gtime.Microsecond() - t)
}

// 测试默认带缓存情况下的数据库查询
func TestGet(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key := []byte("key_" + strconv.Itoa(i))
        db.View(func(tx *bolt.Tx) error {
            b := tx.Bucket([]byte("test"))
            if r := b.Get(key); r == nil {
                fmt.Println("TestGet value not found for index:", i)
            }
            return nil
        })
    }
    fmt.Println("TestGet:", gtime.Microsecond() - t)
}

// 测试默认带缓存情况下的数据库删除
func TestRemove(count int) {
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
    fmt.Println("TestRemove:", gtime.Microsecond() - t)
}


func main() {
    count := 10000
    //TestSet(count)
    TestGet(count)
    //TestRemove(count)

}