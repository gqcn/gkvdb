package main

import (
    "fmt"
    "gitee.com/johng/gkvdb/gkvdb"
    "strconv"
)

func example() {
    // 创建数据库，指定数据库存放目录，数据库名称
    db, err := gkvdb.New("/tmp/gkvdb")
    if err != nil {
        fmt.Println(err)
    }

    // 插入数据
    key   := []byte("name")
    value := []byte("john")
    if err := db.Set(key, value); err != nil {
        fmt.Println(err)
    }

    // 查询数据
    fmt.Println("set    get1:", db.Get(key))

    // 删除数据
    if err := db.Remove(key); err != nil {
        fmt.Println(err)
    }

    // 再次查询数据
    fmt.Println("remove get2:", db.Get(key))

    // 事务操作1
    tx    := db.Begin()
    tx.Set(key, value)
    fmt.Println("tx set      get1:", tx.Get(key))
    tx.Remove(key)
    fmt.Println("tx remove   get2:", tx.Get(key))
    tx.Commit()

    // 事务操作2
    tx     = db.Begin()
    tx.Set(key, value)
    fmt.Println("tx set      get3:", tx.Get(key))
    tx.Commit()

    tx.Remove(key)
    fmt.Println("tx remove   get4:", tx.Get(key))
    tx.Rollback()
    fmt.Println("tx rollback get5:", tx.Get(key))


    // 批量操作，使用事务实现
    // 批量写入
    tx     = db.Begin()
    for i := 0; i < 100; i++ {
        key   := []byte("k_" + strconv.Itoa(i))
        value := []byte("v_" + strconv.Itoa(i))
        tx.Set(key, value)
    }
    tx.Commit()
    // 批量删除
    tx     = db.Begin()
    for i := 0; i < 100; i++ {
        key   := []byte("k_" + strconv.Itoa(i))
        tx.Remove(key)
    }
    tx.Commit()

    // 关闭数据库链接，让GC自动回收数据库相关资源
    db.Close()
}

func main()  {
    db, err := gkvdb.New("/tmp/gkvdb")
    if err != nil {
        fmt.Println(err)
    }
    t, err := db.NewTable("test")
    if err != nil {
        fmt.Println(err)
    }

    //for i := 0; i < 10; i++ {
    //    key   := []byte("k_" + strconv.Itoa(i))
    //    value := []byte("v_" + strconv.Itoa(i))
    //    t.Set(key, value)
    //}

    for i := 0; i < 10; i++ {
        key   := []byte("k_" + strconv.Itoa(i))
        fmt.Println(t.Get(key))
    }
}
