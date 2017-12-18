package main

import (
    "fmt"
    "gitee.com/johng/gkvdb/gkvdb"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
    "strconv"
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

    items := db.Items(1000000)
    fmt.Println(len(items), items)

    //select{}
}