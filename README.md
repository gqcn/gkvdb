Go语言开发的基于DRH(Deep-Re-Hash)深度哈希分区算法的高性能高可用Key-Value嵌入式事务数据库。

## 特点
1. 基于纯Go语言实现，具有优异的跨平台性；
1. 数据库文件采用DRH算法设计，提升对随机数据的操作性能；
1. 良好的IO复用设计，提升对底层数据库文件的操作性能；
1. 良好的高可用设计，保证在任何异常情况下数据的完整性；
1. 提供的基本操作接口：Set()、Get()、Remove()；
1. 提供的事务操作接口：Begin()、Commit()、Rollback()；
1. 提供的多表操作接口：Table()、SetTo()、GetFrom()、RemoveFrom()；
1. 支持原子操作、批量操作、事务操作、多表操作、多表事务、随机遍历等特性；


## 限制
1. (默认)表名最长 255B；
1. (默认)键名最长 255B；
1. (默认)键值最长 16MB；
1. (默认)单表数据 1TB；
1. 支持随机遍历，不支持范围遍历；
1. 嵌入式数据库，没有内置C/S架构；


## 安装
```
go get -u gitee.com/johng/gf
go get -u gitee.com/johng/gkvdb
```


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
t1, err := db.Table("user1")
if err != nil {
    fmt.Println(err)
}
t2, err := db.Table("user2")
if err != nil {
    fmt.Println(err)
}
for i := 0; i < 10; i++ {
    key   := []byte("k_" + strconv.Itoa(i))
    value := []byte("v_" + strconv.Itoa(i))
    t1.Set(key, value)
}
for i := 10; i < 20; i++ {
    key   := []byte("k_" + strconv.Itoa(i))
    value := []byte("v_" + strconv.Itoa(i))
    t2.Set(key, value)
}

fmt.Println(t1.Items(-1))
fmt.Println(t2.Items(-1))
```



