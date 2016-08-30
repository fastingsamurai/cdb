package cdb

import (
	"encoding/binary"
	"io"
)

func (cdb *CDB) readTuple(r io.ReaderAt, offset uint32) (uint32, uint32, error) {
	tuple := cdb.tuplestorage.Get().([]byte)
	_, err := r.ReadAt(tuple, int64(offset))
	if err != nil {
		cdb.tuplestorage.Put(tuple)
		return 0, 0, err
	}

	first := binary.LittleEndian.Uint32(tuple[:4])
	second := binary.LittleEndian.Uint32(tuple[4:])
	cdb.tuplestorage.Put(tuple)
	return first, second, nil
}

func (cdb *Writer) writeTuple(w io.Writer, first, second uint32) error {
	tuple := cdb.tuplestorage.Get().([]byte)
	binary.LittleEndian.PutUint32(tuple[:4], first)
	binary.LittleEndian.PutUint32(tuple[4:], second)

	_, err := w.Write(tuple)
	cdb.tuplestorage.Put(tuple)
	return err
}
