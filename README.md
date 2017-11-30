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

// 创建数据库，指定数据库存放目录，数据库名称
db, err := gkvdb.New("/tmp/gkvdb", "test")
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

#### 4、随机遍历
```go
// 随机获取10条数据
fmt.Println(db.Items(10))

// 获取所有的键值对数据
fmt.Println(db.Items(-1))

// 获取所有的键键名
fmt.Println(db.Keys(-1))

// 获取所有的键键值
fmt.Println(db.Values(-1))
```

## 文档
1. [gkvdb的介绍及设计](http://johng.cn/gkvdb-brief/)
1. [gkvdb v1.5的性能测试及与leveldb的性能对比](http://johng.cn/gkvdb-performance-test-1-5/)




