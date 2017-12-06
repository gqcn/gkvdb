Go语言开发的基于DRH(Deep-Re-Hash)深度哈希分区算法的高性能Key-Value嵌入式数据库。

## 安装
```
go get -u gitee.com/johng/gf
go get -u gitee.com/johng/gkvdb
````


## 使用
#### 1、基本用法
```go
import "gitee.com/johng/gkvdb/gkvdb"

// 创建数据库，指定数据库存放目录
// gkvdb支持多表，默认数据表名称为default
db, err := gkvdb.New("/tmp/gkvdb")
if err != nil {
    fmt.Println(err)
}

key   := []byte("name")
value := []byte("john")

// 插入数据
if err := db.Set(key, value); err != nil {
    fmt.Println(err)
}

// 查询数据
fmt.Println(db.Get(key))

// 删除数据
if err := db.Remove(key); err != nil {
    fmt.Println(err)
}

// 关闭数据库链接，让GC自动回收数据库相关资源
db.Close()
```


#### 2、事务操作
```go
// 开启事务
tx := db.Begin()

// 事务写入
tx.Set(key, value)

// 事务查询
fmt.Println(tx.Get(key))

// 事务提交
tx.Commit()

// 事务删除
tx.Remove(key)

// 事务回滚
tx.Rollback()

// 链式操作
db.Begin().Set(key, value).Commit()
db.Begin().Rmove(key).Commit()
```

#### 3、批量操作
```go
// 批量操作需要使用事务来实现
tx := db.Begin()

// 批量写入
for i := 0; i < 100; i++ {
    key   := []byte("k_" + strconv.Itoa(i))
    value := []byte("v_" + strconv.Itoa(i))
    tx.Set(key, value)
}
tx.Commit()

// 批量删除
for i := 0; i < 100; i++ {
    key   := []byte("k_" + strconv.Itoa(i))
    tx.Remove(key)
}
tx.Commit()
```

#### 4、多表操作
```go
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

// 手动关闭表，释放表资源
// 一般不用手动关闭，在数据库关闭时会自动关闭所有的表
tu.Close()
```


#### 5、多表事务
```go
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
```


#### 6、随机遍历
```go
// ======默认default表的遍历=====
// 随机获取10条数据
fmt.Println(db.Items(10))

// 获取所有的键值对数据
fmt.Println(db.Items(-1))

// 获取所有的键键名
fmt.Println(db.Keys(-1))

// 获取所有的键键值
fmt.Println(db.Values(-1))

// ======指定表的遍历=====
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
```

## 文档
1. [gkvdb的介绍及设计](http://johng.cn/gkvdb-brief/)
1. [gkvdb v1.5的性能测试及与leveldb的性能对比](http://johng.cn/gkvdb-performance-test-1-5/)




