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
	nc := new(Collection)
	nc.cache = make(map[string]I)
	nc.savech = make(chan I)
	nc.halt = make(chan bool)
	nc.finished = make(chan bool)
	nc.directory = db.directory + "/" + name
	nc.template = template
	os.Mkdir(nc.directory, os.ModeDir | 1023)
	db.collections[name] = nc
	fs, err := LoadFileStore(nc.directory)
	if err != nil {
		fs, err = NewFileStore(nc.directory,256,1024)
		if err != nil {
			panic(err)
		}
	}
	nc.store = fs

	nc.readStoredKeys()
	go nc.syncRoutine()
	return nc
}

func (db *Jadb) Close() {
	if r := recover(); r != nil {
		//recovered from panic, now lets clean up
	}
	for _,v := range db.collections {
		v.cleanup()
	}
}

