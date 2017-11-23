Go语言开发的基于DRH(Deep-Re-Hash)深度哈希分区算法的高性能Key-Value嵌入式数据库。

# 安装
```
go get -u gitee.com/johng/gf
go get -u gitee.com/johng/gkvdb
````


# 使用
基本用法
```go
import "gitee.com/johng/gkvdb/gkvdb"

// 创建数据库
db, err := gkvdb.New("/tmp/gkvdb", "test")
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
key   := []byte("john")
fmt.Println(db.Get(key))

// 删除数据
key   := []byte("john")
if err := db.Remove(key); err != nil {
    fmt.Println(err)
}

// 关闭数据库链接，让GC自动回收数据库相关资源
db.Close()


```
开启/关闭缓存
```go
// 启用缓存
db.SetCache(true)

// 关闭缓存
db.SetCache(false)
```

特殊写入操作
```go
// 无论缓存是否开启，直接写入数据到磁盘
key   := []byte("name")
value := []byte("john")
if err := db.SetWithoutCache(key, value); err != nil {
    fmt.Println(err)
}
```


键值对随机遍历
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

# 文档
1. [gkvdb的介绍及设计](http://johng.cn/gkvdb-brief/)
1. [gkvdb的性能测试及与leveldb、boltdb性能对比](http://johng.cn/gkvdb-performance-test/)
