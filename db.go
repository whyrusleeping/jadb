package jadb

import (
	"os"
	"encoding/gob"
)

type I interface {
	GetID() string
}

type SomnDB struct {
	collections map[string]*Collection
	directory string
}

func MakeSomnDB(dir string) *SomnDB {
	db := new(SomnDB)
	db.directory = dir
	db.collections = make(map[string]*Collection)
	os.Mkdir(dir, os.ModeDir | 1023)
	return db
}

func (db *SomnDB) Collection(name string) *Collection {
	c, ok := db.collections[name]
	if ok {
		return c
	}
	nc := new(Collection)
	nc.cache = make(map[string]I)
	nc.getch = make(chan string)
	nc.retch = make(chan I)
	nc.savech = make(chan I)
	nc.halt = make(chan bool)
	nc.directory = db.directory + "/" + name
	os.Mkdir(nc.directory, os.ModeDir | 1023)
	db.collections[name] = nc

	nc.encwrite = new(WriteForwarder)
	nc.enc = gob.NewEncoder(nc.encwrite)
	nc.readStoredKeys()
	go nc.syncRoutine()
	return nc
}

func (db *SomnDB) Close() {
	if r := recover(); r != nil {
		//recovered from panic, now lets clean up
	}
	for _,v := range db.collections {
		v.halt <- true
	}
}

