package main

import (
    "fmt"
    "gitee.com/johng/gkvdb/gkvdb"
    "strconv"
)

// 基本操作
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

// 多表操作
func table() {
    db, err := gkvdb.New("/tmp/gkvdb")
    if err != nil {
        fmt.Println(err)
    }

    // 创建user表
    name    := "user"
    tu, err := db.Table(name)
    if err != nil {
        fmt.Println(err)
    }

    // user表写入数据
    tu.Set([]byte("user_0"), []byte("name_0"))

    // user表查询数据
    fmt.Println(tu.Get([]byte("user_0")))

    // user表删除数据
    tu.Remove([]byte("user_0"))

    // 通过db对象操作user表写入数据
    db.SetTo([]byte("user_1"), []byte("name_1"), name)

    // 通过db对象操作user表查询数据
    fmt.Println(db.GetFrom([]byte("user_1"), name))

    // 通过db对象操作user表删除数据
    db.RemoveFrom([]byte("user_1"), name)
}

// 多表事务
func tabletx() {
    db, err := gkvdb.New("/tmp/gkvdb")
    if err != nil {
        fmt.Println(err)
    }

    // 两张表
    name1 := "user1"
    name2 := "user2"

    // 创建事务对象
    tx := db.Begin()

    // 事务操作user表写入数据
    tx.SetTo([]byte("user_1"), []byte("name_1"), name1)
    tx.SetTo([]byte("user_2"), []byte("name_2"), name2)

    // 事务操作user表查询数据
    fmt.Println("tx get1:", tx.GetFrom([]byte("user_1"), name1))
    fmt.Println("tx get2:", tx.GetFrom([]byte("user_2"), name2))
    tx.Commit()
    fmt.Println("db get1:", db.GetFrom([]byte("user_1"), name1))
    fmt.Println("db get2:", db.GetFrom([]byte("user_2"), name2))

    // 事务操作user表删除数据
    tx.RemoveFrom([]byte("user_1"), name1)
    tx.RemoveFrom([]byte("user_2"), name2)
    fmt.Println("tx removed1:",tx.GetFrom([]byte("user_1"), name1))
    fmt.Println("tx removed2:",tx.GetFrom([]byte("user_2"), name2))
    // 删除操作将被回滚
    tx.Rollback()
    // 重新查询
    fmt.Println("tx get1:", tx.GetFrom([]byte("user_1"), name1))
    fmt.Println("tx get2:", tx.GetFrom([]byte("user_2"), name2))
    fmt.Println("db get1:", db.GetFrom([]byte("user_1"), name1))
    fmt.Println("db get2:", db.GetFrom([]byte("user_2"), name2))
}

// 数据表遍历
func items() {
    db, err := gkvdb.New("/tmp/gkvdb")
    if err != nil {
        fmt.Println(err)
    }

    // 两张表
    name1 := "user1"
    name2 := "user2"
    tu1, err := db.Table(name1)
    if err != nil {
        fmt.Println(err)
    }
    tu2, err := db.Table(name2)
    if err != nil {
        fmt.Println(err)
    }
    for i := 0; i < 10; i++ {
        key   := []byte("k_" + strconv.Itoa(i))
        value := []byte("v_" + strconv.Itoa(i))
        tu1.Set(key, value)
    }
    for i := 10; i < 20; i++ {
        key   := []byte("k_" + strconv.Itoa(i))
        value := []byte("v_" + strconv.Itoa(i))
        tu2.Set(key, value)
    }

    fmt.Println(tu1.Items(-1))
    fmt.Println(tu2.Items(-1))
}

func main()  {
    tabletx()
}
