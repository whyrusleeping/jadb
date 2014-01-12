package jadb

import (
	"sync"
	"encoding/gob"
	"os"
	"bufio"
)

type Collection struct {
	directory string
	cache map[string]I
	getch chan string
	retch chan I
	savech chan I
	halt chan bool

	lock sync.RWMutex

	enc *gob.Encoder
	encwrite *WriteForwarder
}

func (col *Collection) readStoredKeys() error {
	path := col.directory + "/.keys"
	fi, err := os.Open(path)
	if err != nil {
		return err
	}

	scan := bufio.NewScanner(fi)
	for scan.Scan() {
		col.cache[scan.Text()] = nil
	}
	return nil
}

//Loads the value for the given key from the disk and caches it in memory
func (col *Collection) cacheKey(id string) I {
	path := col.directory + "/" + id
	fi, err := os.Open(path)
	if err != nil {
		//TODO: handle error?
		return nil
	}

	var v I
	dec := gob.NewDecoder(fi)
	dec.Decode(v)
	col.cache[id] = v
	fi.Close()
	return v
}

func (col *Collection) FindByID(id string) I {
	col.lock.RLock()
	v, ok := col.cache[id]
	if !ok {
		return nil
	}
	if v == nil {
		v = col.cacheKey(id)
	}
	col.lock.RUnlock()
	return v
}

func (col *Collection) writeDoc(o I) error {
	path := col.directory + "/" + o.GetID()
	fi, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fi.Close()
	//NOTE: this is probably slow as shit, constructing gob encoders
	//is pricey, find a better way
	col.encwrite.SetTarget(fi)
	return col.enc.Encode(o)
}

func (col *Collection) syncRoutine() {
	for {
		select {
			case save := <-col.savech:
				err := col.writeDoc(save)
				if err != nil {
					panic(err) //TODO: dont panic...
				}
			case sig := <-col.halt:
				if len(col.savech) > 0 {
					go func() {col.halt <- sig}()
					continue
				} else {
					break
				}
		}
	}
}

//Thoughts for the future, identify changed fields and only update them
//might be more trouble than its worth though
func (col *Collection) Save(o I) {
	col.lock.Lock()
	col.cache[o.GetID()] = o
	col.savech <- o
	col.lock.Unlock()
}

func (col *Collection) FindWhere(match func(I) bool) []I {
	col.lock.RLock()
	defer col.lock.RUnlock()
	ret := make([]I, 0, 16)
	for id,v := range col.cache {
		if v == nil {
			v = col.cacheKey(id)
		}
		if match(v) {
			ret = append(ret, v)
		}
	}
	return ret
}
