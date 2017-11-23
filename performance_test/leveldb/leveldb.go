package main

import (
    "fmt"
    "gitee.com/johng/gf/g/util/gtime"
    "strconv"
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/opt"
)

var db *leveldb.DB

func init() {
    t := gtime.Microsecond()
    db, _ = leveldb.OpenFile("/tmp/leveldb", nil)
    fmt.Println("db init:", gtime.Microsecond() - t)
}

// 测试默认带缓存情况下的数据库写入
func TestSetWithCache(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key   := []byte("key_" + strconv.Itoa(i))
        value := []byte("value_" + strconv.Itoa(i))
        if err := db.Put(key, value, nil); err != nil {
            fmt.Println(err)
        }
    }
    fmt.Println("TestSetWithCache:", gtime.Microsecond() - t)
}

// 测试默认带缓存情况下的数据库查询
func TestGetWithCache(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key := []byte("key_" + strconv.Itoa(i))
        if r, _ := db.Get(key, nil); r == nil {
            fmt.Println("TestGetWithCache value not found for index:", i)
        }
    }
    fmt.Println("TestGetWithCache:", gtime.Microsecond() - t)
}

// 测试默认带缓存情况下的数据库删除
func TestRemoveWithCache(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key := []byte("key_" + strconv.Itoa(i))
        if err := db.Delete(key, nil); err != nil {
            fmt.Println(err)
        }
    }
    fmt.Println("TestRemoveWithCache:", gtime.Microsecond() - t)
}



// 测试不带缓存情况下的数据库写入
func TestSetWithoutCache(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key   := []byte("key_" + strconv.Itoa(i))
        value := []byte("value_" + strconv.Itoa(i))
        if err := db.Put(key, value, &opt.WriteOptions{Sync:true}); err != nil {
            fmt.Println(err)
        }
    }
    fmt.Println("TestSetWithoutCache:", gtime.Microsecond() - t)
}

// 测试不带缓存情况下的数据库查询
func TestGetWithoutCache(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key := []byte("key_" + strconv.Itoa(i))
        if r, _ := db.Get(key, nil); r == nil {
            fmt.Println("TestGetWithoutCache value not found for index:", i)
        }
    }
    fmt.Println("TestGetWithoutCache:", gtime.Microsecond() - t)
}

// 测试不带缓存情况下的数据库删除
func TestRemoveWithoutCache(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key := []byte("key_" + strconv.Itoa(i))
        if err := db.Delete(key, &opt.WriteOptions{Sync:true}); err != nil {
            fmt.Println(err)
        }
    }
    fmt.Println("TestRemoveWithoutCache:", gtime.Microsecond() - t)
}

func main() {
    var count int = 0

    //
    // ==================不带缓存的KV操作=======================
    // 100W性能测试
    fmt.Println("=======================================100W without cache=======================================")
    count = 1000000
    TestSetWithoutCache(count)
    TestGetWithoutCache(count)
    TestRemoveWithoutCache(count)
    //
    //if err := db.Remove([]byte("key_" + strconv.Itoa(1))); err != nil {
    //    fmt.Println(err)
    //}
    //
    //if err := db.Remove([]byte("key_" + strconv.Itoa(10))); err != nil {
    //    fmt.Println(err)
    //}
    //
    //if err := db.Remove([]byte("key_" + strconv.Itoa(80))); err != nil {
    //    fmt.Println(err)
    //}
    // 500W性能测试
    //fmt.Println("=======================================500W without cache=======================================")
    //count = 5000000
    //TestSetWithoutCache(count)
    //TestGetWithoutCache(count)
    //TestRemoveWithoutCache(count)
    //// 1000W性能测试
    //fmt.Println("=======================================1000W without cache=======================================")
    //count = 10000000
    //TestSetWithoutCache(count)
    //TestGetWithoutCache(count)
    //TestRemoveWithoutCache(count)




    // ==================带缓存的KV操作=======================
    // 100W性能测试
    //fmt.Println("=======================================100W with cache=======================================")
    //count  = 1000000
    //TestSetWithCache(count)
    //TestGetWithCache(count)
    //TestRemoveWithCache(count)

    // 500W性能测试
    //fmt.Println("=======================================500W with cache=======================================")
    //count  = 5000000
    //TestSetWithCache(count)
    //TestGetWithCache(count)
    //TestRemoveWithCache(count)
    //// 1000W性能测试
    //fmt.Println("=======================================1000W with cache=======================================")
    //count  = 10000000
    //TestSetWithCache(count)
    //TestGetWithCache(count)
    //TestRemoveWithCache(count)






    //gtime.SetInterval(time.Second, func() bool {
    //    db.PrintState()
    //    return true
    //})
    //
    ////db.PrintState()
    //
    //select {
    //
    //}
}