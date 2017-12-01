package main

import (
    "fmt"
    "strconv"
    "gitee.com/johng/gkvdb/gkvdb"
    "gitee.com/johng/gf/g/util/gtime"
    "sync"
)

// 数据库对象指针
var db    *gkvdb.DB
// 按照批次执行操作的数量
var batch int = 10000
// 并发数量
var group int = 50

// 数据库初始化
func init() {
    t := gtime.Microsecond()
    db, _ = gkvdb.New("/tmp/gkvdb", "test")
    fmt.Println("db init:", gtime.Microsecond() - t)
}

// 测试数据库写入
func TestSet(count int) {
    var wg sync.WaitGroup
    t  := gtime.Microsecond()
    p  := count/group
    p  += group - p%group
    for g := 0; g < group; g++ {
        ss := g*p + 1
        se := (g + 1)*p
        if se > count {
            se = count
        }
        wg.Add(1)
        go func(start, end int) {
            tx := db.Begin()
            for i := start; i <= end; i++ {
                key   := []byte("key_" + strconv.Itoa(i))
                value := []byte("value_" + strconv.Itoa(i))
                tx.Set(key, value)
                if i % batch == 0 {
                    tx.Commit()
                }
            }
            tx.Commit()
            wg.Done()
        }(ss, se)
    }
    wg.Wait()
    fmt.Println("TestSet:", gtime.Microsecond() - t)
}

// 测试数据库删除
func TestRemove(count int) {
    var wg sync.WaitGroup
    t  := gtime.Microsecond()
    p  := count/group
    p  += group - p%group
    for g := 0; g < group; g++ {
        ss := g*p + 1
        se := (g + 1)*p
        if se > count {
            se = count
        }
        wg.Add(1)
        go func(start, end int) {
            tx := db.Begin()
            for i := start; i <= end; i++ {
                key   := []byte("key_" + strconv.Itoa(i))
                tx.Remove(key)
                if i % batch == 0 {
                    tx.Commit()
                }
            }
            tx.Commit()
            wg.Done()
        }(ss, se)
    }
    wg.Wait()
    fmt.Println("TestRemove:", gtime.Microsecond() - t)
}

// 测试数据库查询
func TestGet(count int) {
    var wg sync.WaitGroup
    t  := gtime.Microsecond()
    p  := count/group
    p  += group - p%group
    for g := 0; g < group; g++ {
        ss := g*p + 1
        se := (g + 1)*p
        if se > count {
            se = count
        }
        wg.Add(1)
        go func(start, end int) {
            for i := start; i <= end; i++ {
                key := []byte("key_" + strconv.Itoa(i))
                if r := db.Get(key); r == nil {
                    fmt.Println("TestGet value not found for index:", i)
                }
            }
            wg.Done()
        }(ss, se)
    }
    wg.Wait()
    fmt.Println("TestGet:", gtime.Microsecond() - t)
}


func main() {
    //count := 5000000
    //TestSet(count)
    //TestGet(count)
    //TestRemove(count)
    //select {

    //}
}