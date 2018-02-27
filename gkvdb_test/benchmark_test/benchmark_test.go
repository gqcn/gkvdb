package bench_test

import (
    "testing"
    "gitee.com/johng/gkvdb/gkvdb"
    "strconv"
)

var db *gkvdb.DB

func init() {
    db, _ = gkvdb.New("/tmp/gkvdb")
}

func BenchmarkSet(b *testing.B) {
    for i := 0; i < b.N; i++ {
        db.Set([]byte("key_" + strconv.Itoa(i)), []byte("value_" + strconv.Itoa(i)))
    }
}

func BenchmarkGet(b *testing.B) {
    for i := 0; i < b.N; i++ {
        db.Get([]byte("key_" + strconv.Itoa(i)))
    }
}

func BenchmarkRemove(b *testing.B) {
    for i := 0; i < b.N; i++ {
        db.Remove([]byte("key_" + strconv.Itoa(i)))
    }
}