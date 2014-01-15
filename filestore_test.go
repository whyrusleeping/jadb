package jadb

import (
	"os"
	"fmt"
	"testing"
	"crypto/rand"
	"io/ioutil"
	"bytes"
)

func TestDatastoreSingle(t *testing.T) {
	os.Mkdir("testData", os.ModeDir | 1023)
	defer os.RemoveAll("testData")
	fs,err := NewFileStore("testData",256,1024)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 500)
	rand.Read(data)
	mb := fs.StoreForKey("random")
	mb.Write(data)
	fs.Close()

	fs, err = LoadFileStore("testData")
	if err != nil {
		t.Fatal(err)
	}
	mb = fs.StoreForKey("random")
	b,err := ioutil.ReadAll(mb)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data,b) {
		fmt.Println(data)
		fmt.Println(b)
		t.Fatal("Data mismatch...")
	}
}

func TestDatastoreMulti(t *testing.T) {
	os.Mkdir("testData", os.ModeDir | 1023)
	defer os.RemoveAll("testData")
	fs,err := NewFileStore("testData",256,1024)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	var datas [][]byte

	for i := 0; i < 50; i++ {
		k := fmt.Sprint(i)
		keys = append(keys, k)
		buf := make([]byte, 1000)
		rand.Read(buf)
		datas = append(datas, buf)
		fi := fs.StoreForKey(k)
		fi.Write(buf)
	}

	fs.Close()

	fs,err = LoadFileStore("testData")
	if err != nil {
		t.Fatal(err)
	}

	for i,v := range keys {
		fi := fs.StoreForKey(v)
		b, err := ioutil.ReadAll(fi)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b,datas[i]) {
			t.Fatal("Data corrupt!")
		}
	}
}

func TestDatastoreGrowth(t *testing.T) {
	os.Mkdir("testData", os.ModeDir | 1023)
	defer os.RemoveAll("testData")
	fs,err := NewFileStore("testData",256,1024)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	var datas [][]byte

	for i := 0; i < 50; i++ {
		k := fmt.Sprint(i)
		keys = append(keys, k)
		buf := make([]byte, 1000)
		rand.Read(buf)
		datas = append(datas, buf)
		fi := fs.StoreForKey(k)
		fi.Write(buf)
	}

	for i,v := range keys {
		fi := fs.StoreForKey(v)
		datas[i] = append(datas[i], make([]byte, 1000)...)
		rand.Read(datas[i])
		fi.Write(datas[i])
	}

	fs.Close()

	fs,err = LoadFileStore("testData")
	if err != nil {
		t.Fatal(err)
	}

	for i,v := range keys {
		fi := fs.StoreForKey(v)
		b, err := ioutil.ReadAll(fi)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b,datas[i]) {
			t.Fatal("Data corrupt!")
		}
	}
}

