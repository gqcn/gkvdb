package main

import (
    "fmt"
    "strconv"
    "gitee.com/johng/gf/g/util/gtime"
    "github.com/syndtr/goleveldb/leveldb"
    "sync"
)

// 数据库对象指针
var db    *leveldb.DB
// 按照批次执行操作的数量
var batch int = 10000
// 并发数量
var group int = 50

func init() {
    t := gtime.Microsecond()
    db, _ = leveldb.OpenFile("/tmp/leveldb", nil)
    fmt.Println("db init:", gtime.Microsecond() - t)
}

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
            b := new(leveldb.Batch)
            for i := start; i <= end; i++ {
                key   := []byte("key_" + strconv.Itoa(i))
                value := []byte("value_" + strconv.Itoa(i))
                b.Put(key, value)
                if i % batch == 0 {
                    db.Write(b, nil)
                    b.Reset()
                }
            }
            db.Write(b, nil)
            b.Reset()
            wg.Done()
        }(ss, se)
    }
    wg.Wait()
    fmt.Println("TestSet:", gtime.Microsecond() - t)
}

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
            b := new(leveldb.Batch)
            for i := start; i <= end; i++ {
                key := []byte("key_" + strconv.Itoa(i))
                b.Delete(key)
                if i % batch == 0 {
                    db.Write(b, nil)
                    b.Reset()
                }
            }
            db.Write(b, nil)
            b.Reset()
            wg.Done()
        }(ss, se)
    }
    wg.Wait()
    fmt.Println("TestRemove:", gtime.Microsecond() - t)
}

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
                if r, _ := db.Get(key, nil); r == nil {
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
    count := 5000000
    //TestSet(count)
    TestGet(count)
    //TestRemove(count)


}