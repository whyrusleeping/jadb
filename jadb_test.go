package main

import (
	"github.com/whyrusleeping/jadb"
	"os"
	"fmt"
	"testing"
	"crypto/rand"
	orand "math/rand"
	"encoding/base32"
)

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
	rand.Read(b)
	return base32.StdEncoding.EncodeToString(b)[:size]
}

func RandObj() *MyObj {
	o := new(MyObj)
	o.Value = RandString(16)
	o.Num = orand.Int()
	o.Contents = RandString(512)
	return o
}

func TestBasic(t *testing.T) {
	db := jadb.MakeSomnDB("testData")
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
	db = jadb.MakeSomnDB("testData")
	col = db.Collection("objects", new(MyObj))
	val := col.FindByID(o.GetID())
	if val == nil {
		t.Fatalf("Could not reload from disk.")
	}
	b = val.(*MyObj)
	if !o.Equals(b) {
		t.Fail()
	}
}

func BenchmarkSaving(b *testing.B) {
	b.StopTimer()
	var objs []*MyObj
	for i := 0; i < b.N; i++ {
		objs = append(objs, RandObj())
	}
	db := jadb.MakeSomnDB("testData")
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
	dba := jadb.MakeSomnDB("testData")
	objs := dba.Collection("objects", new(MyObj))
	for i := 0; i < b.N; i++ {
		o := RandObj()
		o.Value = fmt.Sprintf("%d", i)
		objs.Save(o)
	}
	dba.Close()
	dba = jadb.MakeSomnDB("testData")
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
