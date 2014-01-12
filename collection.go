package jadb

import (
	"sync"
	"encoding/gob"
	"os"
	"fmt"
	"bufio"
)

type Collection struct {
	directory string
	cache map[string]I
	savech chan I
	halt chan bool
	finished chan bool

	lock sync.RWMutex

	enc *gob.Encoder
	encwrite *WriteForwarder
	template I
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

func (col *Collection) cleanup() {
	col.halt <- true
	<-col.finished
	err := col.writeKeyFile()
	if err != nil {
		fmt.Println("Error writing key file!")
	}
}

//Loads the value for the given key from the disk and caches it in memory
func (col *Collection) cacheKey(id string) I {
	path := col.directory + "/" + id
	fi, err := os.Open(path)
	if err != nil {
		//TODO: handle error?
		fmt.Println("Error opening file...")
		fmt.Println(err)
		return nil
	}

	v := col.template.New()
	dec := gob.NewDecoder(fi)
	err = dec.Decode(v)
	if err != nil {
		fmt.Println("Decode Failed!!")
		fmt.Println(err)
	}
	if v == nil {
		fmt.Println("Decoding returned nil value...")
	}
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
		if v == nil {
			fmt.Println("caching failed...")
		}
	}
	col.lock.RUnlock()
	return v
}

//This is very bad, but ill make it better later.
func (col *Collection) writeKeyFile() error {
	path := col.directory + "/.keys"
	fi, err := os.Create(path)
	if err != nil {
		return err
	}
	for i,_ := range col.cache {
		fi.Write([]byte(i + "\n"))
	}
	return nil
}

func (col *Collection) writeDoc(o I) error {
	path := col.directory + "/" + o.GetID()
	fi, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fi.Close()
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
					col.finished <-true
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
