package main

import (
    "fmt"
    "g/util/gtime"
    "./gkvdb"
    "strconv"
    "time"
)

var db *gkvdb.DB

func init() {
    r, _ := gkvdb.New("/tmp/gkvdb", "test")
    db = r
}

// 测试默认带缓存情况下的数据库写入
func TestSetWithCache(count int) {
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key   := []byte("key_" + strconv.Itoa(i))
        value := []byte("value_" + strconv.Itoa(i))
        if err := db.Set(key, value); err != nil {
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
        if r := db.Get(key); r == nil {
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
        if err := db.Remove(key); err != nil {
            fmt.Println(err)
        }
    }
    fmt.Println("TestRemoveWithCache:", gtime.Microsecond() - t)
}



// 测试不带缓存情况下的数据库写入
func TestSetWithoutCache(count int) {
    db.SetCache(false)
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key   := []byte("key_" + strconv.Itoa(i))
        value := []byte("value_" + strconv.Itoa(i))
        if err := db.Set(key, value); err != nil {
            fmt.Println(err)
        }
    }
    fmt.Println("TestSetWithoutCache:", gtime.Microsecond() - t)
}

// 测试不带缓存情况下的数据库查询
func TestGetWithoutCache(count int) {
    db.SetCache(false)
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key := []byte("key_" + strconv.Itoa(i))
        if r := db.Get(key); r == nil {
            fmt.Println("TestGetWithoutCache value not found for index:", i)
        }
    }
    fmt.Println("TestGetWithoutCache:", gtime.Microsecond() - t)
}

// 测试不带缓存情况下的数据库删除
func TestRemoveWithoutCache(count int) {
    db.SetCache(false)
    t := gtime.Microsecond()
    for i := 0; i < count; i++ {
        key := []byte("key_" + strconv.Itoa(i))
        if err := db.Remove(key); err != nil {
            fmt.Println(err)
        }
    }
    fmt.Println("TestRemoveWithoutCache:", gtime.Microsecond() - t)
}

func main() {

    var count int = 0
    // ==================带缓存的KV操作=======================
    // 100W性能测试
    //count  = 1000000
    //TestSetWithCache(count)
    //TestGetWithCache(count)
    //TestRemoveWithCache(count)
    //// 500W性能测试
    //count  = 5000000
    //TestSetWithCache(count)
    //TestGetWithCache(count)
    //TestRemoveWithCache(count)
    //// 1000W性能测试
    //count  = 10000000
    //TestSetWithCache(count)
    //TestGetWithCache(count)
    //TestRemoveWithCache(count)
    //
    //// ==================不带缓存的KV操作=======================
    //// 100W性能测试
    count  = 1
    TestSetWithoutCache(count)
    TestGetWithoutCache(count)
    TestRemoveWithoutCache(count)
    //// 500W性能测试
    //count  = 5000000
    //TestSetWithoutCache(count)
    //TestGetWithoutCache(count)
    //TestRemoveWithoutCache(count)
    //// 1000W性能测试
    //count  = 10000000
    //TestSetWithoutCache(count)
    //TestGetWithoutCache(count)
    //TestRemoveWithoutCache(count)

    time.Sleep(time.Second)

    db.PrintState()

}