package jadb

import (
	"os"
	"fmt"
	"testing"
	"math/rand"
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

func (o *MyObj) New() I {
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

func TestBasic(t *testing.T) {
	db := NewJadb("testData")
	col := db.Collection("objects", new(MyObj))
	o := RandObj()
	col.Save(o)

	//Test basic recall
	b := col.FindByID(o.GetID()).(*MyObj)
	if !o.Equals(b) {
		t.Fail()
	}
	db.Close()

	//Test cold recall
	db = NewJadb("testData")
	col = db.Collection("objects", new(MyObj))
	val := col.FindByID(o.GetID())
	if val == nil {
		t.Fatalf("Could not reload from disk.")
	}
	b = val.(*MyObj)
	if !o.Equals(b) {
		t.Fail()
	}
	os.RemoveAll("testData")
}

func TestMany(t *testing.T) {
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
	db := NewJadb("testData")
	col := db.Collection("objects", new(MyObj))
	for _,v := range list {
		col.Save(v)
	}
	db.Close()

	//Test cold recall
	db = NewJadb("testData")
	col = db.Collection("objects", new(MyObj))
	for _,v := range list {
		val := col.FindByID(v.GetID())
		if val == nil {
			t.Fatalf("Could not reload from disk.")
		}
		b := val.(*MyObj)
		if !v.Equals(b) {
			t.Fail()
		}
	}
	pprof.StopCPUProfile()
	fi.Close()
	os.RemoveAll("testData")
}

func BenchmarkSaving(b *testing.B) {
	b.StopTimer()
	var objs []*MyObj
	for i := 0; i < b.N; i++ {
		objs = append(objs, RandObj())
	}
	db := NewJadb("testData")
	col := db.Collection("objects", new(MyObj))
	b.StartTimer()
	for _,v := range objs {
		col.Save(v)
	}
	db.Close()
	b.StopTimer()
	os.RemoveAll("testData")
}

func BenchmarkReading(b *testing.B) {
	b.StopTimer()
	dba := NewJadb("testData")
	objs := dba.Collection("objects", new(MyObj))
	for i := 0; i < b.N; i++ {
		o := RandObj()
		o.Value = fmt.Sprintf("%d", i)
		objs.Save(o)
	}
	dba.Close()
	dba = NewJadb("testData")
	objs = dba.Collection("objects", new(MyObj))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s := fmt.Sprint(i)
		for j := 0; j < 10; j++ {
			objs.FindByID(s)
		}
	}
	b.StopTimer()
	os.RemoveAll("testData")
}
