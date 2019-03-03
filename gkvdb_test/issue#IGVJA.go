package main

import (
    "fmt"
    "strconv"
    "gitee.com/johng/gkvdb/gkvdb"
    "github.com/gogf/gf/g/os/glog"
    "github.com/gogf/gf/g/os/gtime"
)

func main() {
    db, err := gkvdb.New("/tmp/gkvdb")
    if err != nil {
        glog.Error(err)
    }

    t1 := gtime.Microsecond()
    for i := 0; i < 200001; i++ {
        key   := []byte("name" + strconv.Itoa(i))
        value := []byte("john" + strconv.Itoa(i))
        if err := db.Set(key, value); err != nil {
            glog.Error(err)
        }
    }
    fmt.Println("set cost:", gtime.Microsecond() - t1)

    t2 := gtime.Microsecond()
    for i := 0; i < 200000; i++ {
        key := []byte("name" + strconv.Itoa(i))
        db.Remove(key)
    }
    fmt.Println("remove cost:", gtime.Microsecond() - t2)

    //t3 := gtime.Microsecond()
    //for i := 0; i < 200000; i++ {
    //    key := []byte("name" + strconv.Itoa(i))
    //    if r := db.Get(key); r != nil {
    //        fmt.Println("value found for:", string(key))
    //    }
    //}
    //fmt.Println("get cost:", gtime.Microsecond() - t3)


    items := db.Items(200000)
    fmt.Println("items:", len(items), items)
}