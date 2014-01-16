package main

import (
	"os"
	"math/rand"
	"github.com/whyrusleeping/jadb"
	"runtime/pprof"
)

var src string = "abcdefghijklmnopqrstuvwxyz1234567890"
type MyObj struct {
	Value string
	Num int
	Contents string
}

func (o *MyObj) GetID() string {
	return o.Value
}

func (o *MyObj) New() jadb.I {
	return new(MyObj)
}

func (o *MyObj) Equals(m *MyObj) bool {
	return o.Value == m.Value && o.Num == m.Num && o.Contents == m.Contents
}

func RandString(size int) string {
	b := make([]byte, size)
	for i,_ := range b {
		b[i] = src[rand.Intn(len(src))]
	}
	return string(b)
}

func RandObj() *MyObj {
	o := new(MyObj)
	o.Value = RandString(16)
	o.Num = rand.Int()
	o.Contents = RandString(2048)
	return o
}

func main() {
	fi,err := os.Create("out.prof")
	if err != nil {
		panic(err)
	}
	var list []*MyObj
	for i := 0; i < 100000; i++ {
		o := RandObj()
		list = append(list, o)
	}
	pprof.StartCPUProfile(fi)
	db := jadb.NewJadb("testData")
	col := db.Collection("objects", new(MyObj))
	for _,v := range list {
		col.Save(v)
	}
	db.Close()

	//Test cold recall
	db = jadb.NewJadb("testData")
	col = db.Collection("objects", new(MyObj))
	for _,v := range list {
		val := col.FindByID(v.GetID())
		if val == nil {
			panic("Could not reload from disk.")
		}
		b := val.(*MyObj)
		if !v.Equals(b) {
			panic("not equal!")
		}
	}
	pprof.StopCPUProfile()
	fi.Close()
	os.RemoveAll("testData")
}
