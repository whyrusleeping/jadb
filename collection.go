package jadb

import (
	"sync"
	"encoding/json"
	"io"
	"os"
	"fmt"
)

//A collection is a table in the database for a single struct type
//it manages storing data in its given directory and disk writes
//occur in a non-blocking manner
type Collection struct {
	directory string
	cache map[string]I
	savech chan I
	halt chan bool
	finished chan bool
	store *FileStore

	lock sync.RWMutex

	template I
}

func (col *Collection) readStoredKeys() error {
	path := col.directory + "/.keys"
	fi, err := os.Open(path)
	if err != nil {
		return err
	}

	num := make([]byte, 2)
	buf := make([]byte, 512)
	for {
		_, err := fi.Read(num)
		if err == io.EOF {
			break
		}
		l := (int(num[0]) << 8) + int(num[1])
		if l > cap(buf) {
			buf = buf[:cap(buf)]
			buf = append(buf, make([]byte, (l - len(buf)) * 2)...)
		}
		buf = buf[:l]
		fi.Read(buf)
		col.cache[string(buf)] = nil
	}
	return nil
}

//Wait for all disk writes to finish and then write out final key file
func (col *Collection) cleanup() {
	col.halt <- true
	<-col.finished
	err := col.writeKeyFile()
	if err != nil {
		fmt.Println("Error writing key file!")
	}
	col.store.Close()
}

//Loads the value for the given key from the disk and caches it in memory
func (col *Collection) cacheKey(id string) I {
	fi := col.store.StoreForKey(id)

	v := col.template.New()
	dec := json.NewDecoder(fi)
	err := dec.Decode(v)
	if err != nil {
		//TODO: more handling
		panic(err)
	}
	if v == nil {
		//TODO: handle handle handle
		fmt.Println("Decoding returned nil value...")
	}
	col.cache[id] = v
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
//if there are millions of keys, we read in all of them
//and that might be unecessary.
func (col *Collection) writeKeyFile() error {
	path := col.directory + "/.keys"
	fi, err := os.Create(path)
	if err != nil {
		return err
	}
	for i,_ := range col.cache {
		fi.Write([]byte{byte(len(i)/256), byte(len(i) % 256)})
		fi.Write([]byte(i))
	}
	return nil
}

func (col *Collection) writeDoc(o I) error {
	fi := col.store.StoreForKey(o.GetID())

	enc := json.NewEncoder(fi)
	err := enc.Encode(o)
	return err
}

//Do all disk writes in a separate thread, and in the order
//that they are queued. 
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
