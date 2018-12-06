package main

import (
    "os"
    "path"
    "time"

    "fmt"

    "gitee.com/johng/gkvdb/gkvdb"
)

func main() {
    dbPath := path.Join(os.TempDir(), "testxxxx.db")
    err := os.RemoveAll(dbPath)
    err = os.RemoveAll(dbPath + ".lock")
    if err != nil {
        panic(err)
    }
    db, err := gkvdb.New(dbPath)
    if err != nil {
        panic(err)
    }
    key   := []byte("key")
    value := []byte("value")
    table, err := db.Table("TestTable")
    if err != nil {
        panic(err)
    }
    err = table.Set(key[:], value[:])
    if err != nil {
       panic(err)
    }
    go func() {
        for {
            tx := db.Begin("TestTable")
            err = tx.Set(key[:], value[:])
            if err = tx.Commit(true); err != nil {
                panic(err)
            }
            time.Sleep(100 * time.Millisecond)
        }
    }()
    count := 0
    for {
        count++
        if buf := table.Items(-1); len(buf) == 0 {
            fmt.Printf("异常, len(buf)=0 count = %d err=%v\n", count, err)
        }
    }
}
