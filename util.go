package cdb

import (
	"encoding/binary"
	"io"
	"sync"
)

type Pool struct {
	lock sync.Mutex
	bufs [][]byte
}

func (p *Pool) Get() []byte {
	p.lock.Lock()
	l := len(p.bufs)
	if l == 0 {
		r := make([]byte, 8)
		p.lock.Unlock()
		return r
	} else {
		r := p.bufs[l-1]
		p.bufs = p.bufs[:l-1]
		p.lock.Unlock()
		return r
	}
}

func (p *Pool) Put(buf []byte) {
	p.lock.Lock()
	p.bufs = append(p.bufs, buf)
	p.lock.Unlock()
}

func (cdb *CDB) readTuple(r io.ReaderAt, offset uint32) (uint32, uint32, error) {
	tuple := cdb.tuplepool.Get()
	_, err := r.ReadAt(tuple, int64(offset))
	if err != nil {
		cdb.tuplepool.Put(tuple)
		return 0, 0, err
	}

	first := binary.LittleEndian.Uint32(tuple[:4])
	second := binary.LittleEndian.Uint32(tuple[4:])
	cdb.tuplepool.Put(tuple)
	return first, second, nil
}

var statictuple []byte = make([]byte, 8)

func (cdb *Writer) writeTuple(w io.Writer, first, second uint32) error {
	binary.LittleEndian.PutUint32(statictuple[:4], first)
	binary.LittleEndian.PutUint32(statictuple[4:], second)
	_, err := w.Write(statictuple)
	return err
}
