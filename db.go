package jadb

import (
	"os"
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

