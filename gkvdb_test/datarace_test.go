package main

import (
    "testing"
    "gitee.com/johng/gkvdb/gkvdb"
)

// issue https://gitee.com/johng/gkvdb/issues/IGML6
func Test_Database(t *testing.T) {
    db, err := gkvdb.New("/tmp/gkvdb_data_race")
    if err != nil {
        t.Error(err)
        return
    }
    defer db.Close()

    for i := 0; i < 100000; i++ {
        db.Set([]byte("store:files:" + string(i)), []byte("123456"))
    }

    data := db.Get([]byte("store:files:" + string(99999)))
    if string(data) != "123456" {
        t.Error("get wrong data", data)
        return
    }
}