package jadb

import (
	"os"
)

type FileStore struct {
	fi *os.File
	index *os.File
	objs map[string][]int
	blocks []bool
	blocksize int
}

func (fs *FileStore) getEmptyBlock() (int,error) {
	for i,v := range fs.blocks {
		if !v {
			fs.blocks[i] = true
			return i,nil
		}
	}
	i := len(fs.blocks)
	err := fs.extend()
	if err != nil {
		panic(err)
	}
	fs.blocks[i] = true
	return i,nil
}

func (fs *FileStore) extend() error {
	cursize := len(fs.blocks) * fs.blocksize
	err := fs.fi.Seek(cursize, os.SEEK_SET)
	if err != nil {
		return err
	}
	blockstogrow := len(fs.blocks) / 2
	bytestogrow := blockstogrow * fs.blocksize
	blank := make([]byte, 128)
	for i := 0; i < bytestogrow / 128; i++ {
		err := fs.fi.Write(blank)
		if err != nil {
			//TODO: handle THIS better
			return err
		}
	}
	fs.blocks = append(fs.blocks, make([]bool, blockstogrow)...)
	return nil
}

func (fs *FileStore) writeBytesForKey(key string, offset int, b []byte) error {

}

//Memblock implements Read/Writer for a given key in the filestore
type MemBlock struct {
	key string
	offset int
	in *FileStore
}

func (mb *MemBlock) Write(b []byte) (int, error) {
	return 0,nil
}

func (mb *MemBlock) Read(b []byte) (int, error) {
	return 0,nil
}

