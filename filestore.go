package jadb

import (
	"os"
	"errors"
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

func (fs *FileStore) StoreForKey(key string) *MemBlock {
	mb := new(MemBlock)
	mb.in = fs
	mb.key = key
	return mb
}

func (fs *FileStore) extend() error {
	cursize := int64(len(fs.blocks) * fs.blocksize)
	_,err := fs.fi.Seek(cursize, os.SEEK_SET)
	if err != nil {
		return err
	}
	blockstogrow := len(fs.blocks) / 2
	bytestogrow := blockstogrow * fs.blocksize
	blank := make([]byte, 128)
	for i := 0; i < bytestogrow / 128; i++ {
		_,err := fs.fi.Write(blank)
		if err != nil {
			//TODO: handle THIS better
			return err
		}
	}
	fs.blocks = append(fs.blocks, make([]bool, blockstogrow)...)
	return nil
}

func (fs *FileStore) DeleteKey(key string) error {
	blks, ok := fs.objs[key]
	if !ok {
		return errors.New("Key not found!")
	}
	for _,v := range blks {
		fs.blocks[v] = false
	}
	delete(fs.objs, key)
	return nil
}

func (fs *FileStore) writeBlock(bnum, offset int, b []byte) (int, error) {
	pos := int64((bnum * fs.blocksize) + offset)
	_,err := fs.fi.Seek(pos, os.SEEK_SET)
	if err != nil {
		return 0, err
	}
	return fs.fi.Write(b)
}

func (fs *FileStore) readBlock(bnum, offset int, b []byte) (int, error) {
	pos := int64((bnum * fs.blocksize) + offset)
	_,err := fs.fi.Seek(pos, os.SEEK_SET)
	if err != nil {
		return 0, err
	}
	return fs.fi.Read(b)
}

func (fs *FileStore) readByteForKey(key string, offset int, b []byte) (int,error) {
	blks, ok := fs.objs[key]
	if !ok {
		return 0, errors.New("Invalid store position!")
	}
	needend := offset + len(b)
	haveend := len(blks) * fs.blocksize
	if needend > haveend {
		b = b[:haveend-offset]
	}

	rblk := offset / fs.blocksize
	ri := offset % fs.blocksize
	for read := 0; read < len(b); {
		toread := fs.blocksize - ri
		if len(b) < fs.blocksize - ri {
			toread = len(b)
		}
		n, err := fs.readBlock(blks[rblk], ri, b[:toread])
		if err != nil {
			return n+read,err
		}
		read += n
		ri += n
		b = b[toread:]
		if ri == fs.blocksize {
			rblk++
			ri = 0
		}
	}
	return len(b), nil
}

func (fs *FileStore) writeBytesForKey(key string, offset int, b []byte) (int,error) {
	blks, ok := fs.objs[key]
	if !ok {
		blks = []int{}
		fs.objs[key] = blks
	}
	needend := offset + len(b)
	haveend := len(blks) * fs.blocksize
	for needend > haveend {
		i,err := fs.getEmptyBlock()
		if err != nil {
			//TODO: probably not just return here
			return 0,err
		}
		blks = append(blks, i)
		haveend += fs.blocksize
	}
	fs.objs[key] = blks

	wrblk := offset / fs.blocksize
	wri := offset % fs.blocksize
	for written := 0; written < len(b); {
		towrite := fs.blocksize - wri
		if len(b) < fs.blocksize - wri {
			towrite = len(b)
		}
		n, err := fs.writeBlock(blks[wrblk], wri, b[:towrite])
		if err != nil {
			return n+written,err
		}
		written += n
		b = b[towrite:]
		wri += n
		if wri == fs.blocksize {
			wrblk++
			wri = 0
		}
	}
	return len(b), nil
}

//Memblock implements Read/Writer for a given key in the filestore
type MemBlock struct {
	key string
	offset int
	in *FileStore
}

func (mb *MemBlock) Write(b []byte) (int, error) {
	n, err := mb.in.writeBytesForKey(mb.key, mb.offset, b)
	mb.offset += n
	return n, err
}

func (mb *MemBlock) Read(b []byte) (int, error) {
	n, err := mb.in.readByteForKey(mb.key, mb.offset, b)
	mb.offset += n
	return n, err
}

