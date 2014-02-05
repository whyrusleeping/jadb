package jadb

import (
	"os"
	"fmt"
	"crypto/rand"
	"crypto/md5"
	"encoding/hex"
	"io"
	"time"
)

type I interface {
	GetID() string
	New() I
}

type Jadb struct {
	collections map[string]*Collection
	directory string
}

func NewJadb(dir string) *Jadb {
	db := new(Jadb)
	db.directory = dir
	db.collections = make(map[string]*Collection)
	os.Mkdir(dir, os.ModeDir | 1023)
	return db
}

func (db *Jadb) Collection(name string, template I) *Collection {
	c, ok := db.collections[name]
	if ok {
		return c
	}
	if template == nil {
		panic("No Template given for collection!")
	}
	nc, err := OpenCollection(db, name, template)
	if err != nil {
		panic(err)
		return nil
	}
	db.collections[name] = nc
	return nc
}

func (db *Jadb) Close() {
	for _,v := range db.collections {
		v.cleanup()
	}
}

func GetUniqueID() string {
	h := md5.New()
	t := time.Now().UnixNano()
	fmt.Fprintf(h, "time:%d", t)
	io.CopyN(h, rand.Reader, 32)
	out := h.Sum(nil)
	return hex.EncodeToString(out)
}
