package main

import (
    "encoding/binary"
    "fmt"
    "github.com/gogf/gf/g/os/gfile"
    "gitee.com/johng/gkvdb/gkvdb"
)

const (
    PidLen      = 8
    SIndexLen   = 1
    KeyLen      = PidLen + SIndexLen
    PTIdLen     = 8
    SILen       = 8
    RelValueLen = PTIdLen + SILen
)

type Relation struct {
    Pid    uint64
    SIndex byte
    PT     uint64
    SI     uint64
}

func main() {
    fileName := gfile.TempDir() + "testdata"
    fmt.Println(fileName)
    db, err := gkvdb.New(fileName)
    if err != nil {
        fmt.Println(err)
        return
    }

    rels := []Relation{
        Relation{
            Pid:    0x00f65483de1fae44,
            SI:     1,
            SIndex: 0,
            PT:     0x037f363f03c0a00,
        },
        Relation{
            Pid:    0x00f65483de1fae44,
            SI:     2,
            SIndex: 1,
            PT:     0x037f363f03c0a00,
        },
        Relation{
            Pid:    0x00f65483de1fae44,
            SI:     3,
            SIndex: 2,
            PT:     0x037f363f03c0a00,
        },
        Relation{
            Pid:    0x00f65483de1fae44,
            SI:     4,
            SIndex: 3,
            PT:     0x037f363f03c0a00,
        },
    }
    buf := make([]byte, KeyLen)
    val := make([]byte, RelValueLen)
    fmt.Println("writing:")
    for _, rel := range rels {
       binary.BigEndian.PutUint64(buf, rel.Pid)
       buf[SILen] = rel.SIndex
       binary.BigEndian.PutUint64(val, rel.PT)
       binary.BigEndian.PutUint64(val[PTIdLen:], rel.SI)
       fmt.Printf("%x, %x\n", buf, val)
       if err := db.Set(buf, val); err != nil {
           fmt.Println(err)
           return
       }

       val := db.Get(buf)
       fmt.Printf("%x, %x\n", buf, val)
    }
    fmt.Println("reading:")
    for _, rel := range rels {
        binary.BigEndian.PutUint64(buf, rel.Pid)
        buf[SILen] = rel.SIndex
        fmt.Printf("%x, %x\n", buf, db.Get(buf))
    }

}
