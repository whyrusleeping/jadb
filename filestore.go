package jadb

import (
	//"fmt"
	"time"
	"os"
	"errors"
	"encoding/json"
	"io"
)

type FileStore struct {
	fi *os.File
	objs map[string]*fsdoc
	freemap []bool
	lastfree int
	blocks []*block
	docaching bool

	blocksize int
	dir string
}

type block struct {
	cache []byte
	last time.Time
}

type fsdoc struct {
	Blocks []int
	Size int64
}

type indexFile struct {
	NBlocks int
	Free []byte
	Docs map[string]*fsdoc
	Blocksize int
}

func BoolsToBitmap(b []bool) []byte {
	nbytes := len(b) / 8
	if len(b) % 8 != 0 {
		nbytes++
	}
	out := make([]byte, nbytes)
	for i,v := range b {
		if v {
			out[i/8] |= byte(1 << (uint(i)%8))
		}
	}
	return out
}

func BitmapToBools(b []byte) []bool {
	out := make([]bool, len(b) * 8)
	outi := 0
	for _,v := range b {
		for i := 0; i < 8; i++ {
			out[outi] = ((v & (1 << uint(i))) == 1)
		}
	}
	return out
}

func (fs *FileStore) Close() error {
	out, err := os.Create(fs.dir + "/index")
	if err != nil {
		return err
	}
	fsdex := fs.getIndexStruct()
	enc := json.NewEncoder(out)
	enc.Encode(fsdex)
	if fs.docaching {
		fs.writeCache()
	}
	fs.fi.Close()
	out.Close()
	return nil
}

func (fs *FileStore) writeCache() error {
	//fmt.Println("writing cache!")
	fs.fi.Seek(0,os.SEEK_SET)
	skip := 0
	for _,v := range fs.blocks {
		if v == nil || v.cache == nil {
			skip++
		} else {
			//fmt.Printf("Writing block: %d\n", i)
			//fmt.Println(v.cache)
			if skip > 0 {
				fs.fi.Seek(int64(skip * fs.blocksize), os.SEEK_CUR)
			}
			_,err := fs.fi.Write(v.cache)
			if err != nil {
				panic(err)
			}
		}
	}
	return nil
}

func (fs *FileStore) getIndexStruct() *indexFile {
	fsdex := new(indexFile)
	fsdex.Blocksize = fs.blocksize
	fsdex.Free = BoolsToBitmap(fs.freemap)
	fsdex.NBlocks = len(fs.freemap)
	fsdex.Docs = fs.objs
	return fsdex
}

func NewFileStore(dir string, blocksize, nblocks int) (*FileStore, error) {
	index, err := os.Create(dir + "/index")
	if err != nil {
		return nil, err
	}
	fsdex := new(indexFile)
	fsdex.Blocksize = blocksize
	fsdex.Docs = make(map[string]*fsdoc)
	fsdex.Free = make([]byte, (nblocks + 8)/8)
	fsdex.NBlocks = nblocks

	enc := json.NewEncoder(index)
	enc.Encode(fsdex)
	index.Close()

	mem, err := os.Create(dir + "/data")
	blank := make([]byte, 4096)
	for i := 0; i < (blocksize * nblocks) / 4096; i++ {
		mem.Write(blank)
	}
	if err != nil {
		return nil, err
	}
	fs := new(FileStore)
	fs.dir = dir
	fs.blocksize = blocksize
	fs.objs = fsdex.Docs
	fs.freemap = make([]bool, nblocks)
	fs.blocks = make([]*block, nblocks)
	fs.fi = mem
	fs.docaching = true
	return fs,nil
}

func LoadFileStore(dir string) (*FileStore, error) {
	index, err := os.Open(dir + "/index")
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(index)
	fsdex := new(indexFile)
	err = dec.Decode(fsdex)
	if err != nil {
		return nil, err
	}
	index.Close()

	fs := new(FileStore)
	fs.freemap = BitmapToBools(fsdex.Free)[:fsdex.NBlocks]
	fs.blocks = make([]*block, len(fs.freemap))
	fs.objs = fsdex.Docs
	fs.blocksize = fsdex.Blocksize
	fs.docaching = true

	mem, err := os.Open(dir + "/data")
	if err != nil {
		return nil, err
	}

	fs.fi = mem
	return fs, nil
}

func (fs *FileStore) getEmptyBlock() (int,error) {
	//fmt.Println("getting new block")
	for i := fs.lastfree; i < len(fs.freemap); i++ {
		if !fs.freemap[i] {
			fs.freemap[i] = true
			fs.lastfree = i + 1
			return i,nil
		}
	}
	i := len(fs.freemap)
	err := fs.extend()
	if err != nil {
		panic(err)
	}
	fs.freemap[i] = true
	return i,nil
}

func (fs *FileStore) StoreForKey(key string) *MemBlock {
	doc,ok := fs.objs[key]
	if !ok {
		doc = new(fsdoc)
		fs.objs[key] = doc
	}
	mb := new(MemBlock)
	mb.in = fs
	mb.key = key
	mb.d = doc
	return mb
}

func (fs *FileStore) extend() error {
	//fmt.Println("Extending mem block.")
	cursize := int64(len(fs.freemap) * fs.blocksize)
	_,err := fs.fi.Seek(cursize, os.SEEK_SET)
	if err != nil {
		return err
	}
	blockstogrow := len(fs.freemap) * 2
	bytestogrow := blockstogrow * fs.blocksize
	for blockstogrow < (1024 * 1024) {
		blockstogrow++
		bytestogrow = blockstogrow * fs.blocksize
	}
	blank := make([]byte, bytestogrow)
	_,err = fs.fi.Write(blank)
	if err != nil {
		//TODO: handle THIS better
		return err
	}
	/*
	for i := 0; i < bytestogrow / 4096; i++ {
		_,err := fs.fi.Write(blank)
		if err != nil {
			//TODO: handle THIS better
			return err
		}
	}*/
	fs.freemap = append(fs.freemap, make([]bool, blockstogrow)...)
	fs.blocks = append(fs.blocks, make([]*block, blockstogrow)...)
	return nil
}

func (fs *FileStore) DeleteKey(key string) error {
	doc, ok := fs.objs[key]
	if !ok {
		return errors.New("Key not found!")
	}
	for _,v := range doc.Blocks {
		fs.freemap[v] = false
		if v < fs.lastfree {
			fs.lastfree = v
		}
	}
	delete(fs.objs, key)
	return nil
}

func (fs *FileStore) writeBlock(bnum, offset int, b []byte) (int, error) {
	//fmt.Printf("writing %d bytes in block %d offset %d\n", len(b), bnum, offset)
	if fs.docaching {
		blk := fs.blocks[bnum]
		if blk == nil {
			//fmt.Println("Doing things here...")
			blk = new(block)
			fs.blocks[bnum] = blk
		}
		if blk.cache == nil {
			//fmt.Println("Allocating cache for block.")
			blk.cache = make([]byte, fs.blocksize)
		}
		//fmt.Println(b)
		copy(blk.cache[offset:], b)
		return len(b),nil
	} else {
		pos := int64((bnum * fs.blocksize) + offset)
		_,err := fs.fi.Seek(pos, os.SEEK_SET)
		if err != nil {
			return 0, err
		}
		return fs.fi.Write(b)
	}
}

func (fs *FileStore) readBlock(bnum, offset int, b []byte) (int, error) {
	//fmt.Printf("reading %d bytes in block %d offset %d\n", len(b), bnum, offset)
	if fs.docaching {
		blk := fs.blocks[bnum]
		if blk == nil {
			blk = new(block)
			fs.blocks[bnum] = blk
		}
		if blk.cache == nil {
			//fmt.Println("Nil cache block, reading from disk...")
			blk.cache = make([]byte, fs.blocksize)
			fs.fi.Seek(int64(bnum * fs.blocksize), os.SEEK_SET)
			fs.fi.Read(blk.cache)
			//fmt.Println(blk.cache)
		}
		//fmt.Printf("offset: %d\n", offset)
		//fmt.Println(blk.cache[offset:])
		copy(b,blk.cache[offset:])
		return len(b),nil
	} else {
		//fmt.Println("Shouldnt be here...")
		pos := int64((bnum * fs.blocksize) + offset)
		_,err := fs.fi.Seek(pos, os.SEEK_SET)
		if err != nil {
			return 0, err
		}
		return fs.fi.Read(b)
	}
}

func (fs *FileStore) readByteForKey(key string, offset int64, b []byte) (int,error) {
	//fmt.Println("Read byte for key.")
	doc, ok := fs.objs[key]
	if !ok {
		return 0, errors.New("Invalid store position!")
	}
	needend := offset + int64(len(b))
	haveend := len(doc.Blocks) * fs.blocksize
	if needend > int64(haveend) {
		b = b[:haveend-int(offset)]
	}

	rblk := offset / int64(fs.blocksize)
	ri := int(offset % int64(fs.blocksize))
	var read int
	max := len(b)
	for read < max {
		//fmt.Printf("Entering read loop: [%d/%d]\n", read, len(b))
		toread := fs.blocksize - ri
		if len(b) < fs.blocksize - ri {
			toread = len(b)
		}
		n, err := fs.readBlock(doc.Blocks[rblk], ri, b[:toread])
		//fmt.Printf("Block read returned %d\n", n)
		if err != nil {
			return n+read,err
		}
		read += n
		ri += n
		b = b[toread:]
		if ri == fs.blocksize {
			//fmt.Println("Advanced block count.")
			rblk++
			ri = 0
		}
	}
	return read, nil
}

func (fs *FileStore) writeBytesForKey(key string, offset int64, b []byte) (int,error) {
	d, ok := fs.objs[key]
	if !ok {
		d = new(fsdoc)
		d.Blocks = []int{}
		fs.objs[key] = d
	}
	needend := offset + int64(len(b))
	haveend := int64(len(d.Blocks) * fs.blocksize)
	for needend > haveend {
		i,err := fs.getEmptyBlock()
		//fmt.Printf("Allocated block: %d\n", i)
		if err != nil {
			//TODO: probably not just return here
			return 0,err
		}
		d.Blocks = append(d.Blocks, i)
		haveend += int64(fs.blocksize)
	}
	fs.objs[key] = d

	wrblk := offset / int64(fs.blocksize)
	wri := int(offset) % fs.blocksize
	var written int
	max := len(b)
	for written < max {
		towrite := fs.blocksize - wri
		if len(b) < fs.blocksize - wri {
			towrite = len(b)
		}
		n, err := fs.writeBlock(d.Blocks[wrblk], wri, b[:towrite])
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
	return written, nil
}

//Memblock implements Read/Writer for a given key in the filestore
type MemBlock struct {
	key string
	offset int64
	in *FileStore
	d *fsdoc
}

func (mb *MemBlock) Write(b []byte) (int, error) {
	////fmt.Printf("Calling write for %d bytes.\n", len(b))
	n, err := mb.in.writeBytesForKey(mb.key, mb.offset, b)
	//fmt.Printf("writebytesforkey returned %d\n", n)
	mb.offset += int64(n)
	if mb.offset > mb.d.Size {
		mb.d.Size = mb.offset
	}
	return n, err
}

func (mb *MemBlock) Read(b []byte) (int, error) {
	if int64(len(b)) + mb.offset > mb.d.Size {
		b = b[:mb.d.Size-mb.offset]
	}
	//fmt.Printf("Calling read: %d/%d\n", mb.offset, mb.d.Size)
	n, err := mb.in.readByteForKey(mb.key, mb.offset, b)
	if n == 0 {
		return 0, io.EOF
	}
	//fmt.Printf("readbyteforkey returned %d\n", n)
	mb.offset += int64(n)
	return n, err
}

