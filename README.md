Go语言开发的基于DRH(Deep-Re-Hash)深度哈希分区算法的高性能Key-Value嵌入式数据库。

# 安装
```
go get -u gitee.com/johng/gf
go get -u gitee.com/johng/gkvdb
````


# 使用
```go
import "gitee.com/johng/gkvdb/gkvdb"

// 创建数据库
db, err := gkvdb.New("/tmp/gkvdb", "test")
if err != nil {
    fmt.Println(err)
}

// 插入数据
key   := []byte("john")
value := []byte("johng.cn")
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

// 启用缓存
db.SetCache(true)

// 关闭缓存
db.SetCache(false)
```


# 文档
1. [gkvdb的介绍及设计](http://johng.cn/gkvdb-brief/)
1. [gkvdb的性能测试](http://johng.cn/gkvdb-performance-test/)
