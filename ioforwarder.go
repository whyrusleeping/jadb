package jadb

import "io"

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
