package jadb

import "io"

//These objects are used when you need to create another object
//that takes a Reader or Writer as an argument, but you want to
//be able to change what is being Read or Written to at will.
//My use case is for gob Encoders, they are expensive to make.
type WriteForwarder struct {
	w io.Writer
}

func (w *WriteForwarder) Write(b []byte) (int, error) {
	return w.w.Write(b)
}

func (w *WriteForwarder) SetTarget(ntarget io.Writer) {
	w.w = ntarget
}

type ReadForwarder struct {
	r io.Reader
}

func (r *ReadForwarder) Read(b []byte) (int, error) {
	return r.r.Read(b)
}

func (r *ReadForwarder) SetTarget(ntarget io.Reader) {
	r.r = ntarget
}
