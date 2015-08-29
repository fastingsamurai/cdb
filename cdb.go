/*
Package cdb provides a native implementation of cdb, a constant key/value
database with some very nice properties.

For more information on cdb, see the original design doc at http://cr.yp.to/cdb.html.
*/
package cdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

// CDB represents an open CDB database. It can only be used for reads; to
// create a database, use Writer.
type CDB struct {
	file   *os.File
	tables [256]table
}

type table struct {
	position uint32
	length   uint32
}

// Open opens an existing CDB database at the given path.
func Open(path string) (*CDB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	cdb := &CDB{file: f}
	err = cdb.readIndex()
	if err != nil {
		return nil, err
	}

	return cdb, nil
}

// Get returns the value for a given key, or nil if it can't be found.
func (cdb *CDB) Get(key []byte) ([]byte, error) {
	digest := newCDBHash()
	digest.Write(key)
	hash := digest.Sum32()

	table := cdb.tables[hash&0xff]
	if table.length == 0 {
		return nil, nil
	}

	// Probe the given hash table, starting at the given slot.
	slot := (hash >> 8) % table.length

	for {
		slotOffset := table.position + (8 * slot)
		_, err := cdb.file.Seek(int64(slotOffset), os.SEEK_SET)
		if err != nil {
			return nil, err
		}

		slotHash, offset, err := cdb.readTuple()
		if err != nil {
			return nil, err
		}

		// An empty slot means the key doesn't exist.
		if slotHash == 0 {
			break
		} else if slotHash == hash {
			value, err := cdb.getValueAt(offset, key)
			if err != nil {
				return nil, err
			} else if value != nil {
				return value, nil
			}
		}

		slot = (slot + 1) % table.length
	}

	return nil, nil
}

func (cdb *CDB) readIndex() error {
	headerLength := 256 * 8
	buf := make([]byte, headerLength)
	_, err := io.ReadFull(cdb.file, buf)
	if err != nil {
		return err
	}

	for i := 0; i < 256; i++ {
		off := i * 8
		position := binary.LittleEndian.Uint32(buf[off : off+4])
		length := binary.LittleEndian.Uint32(buf[off+4 : off+8])
		cdb.tables[i] = table{position: position, length: length}
	}

	return nil
}

func (cdb *CDB) getValueAt(offset uint32, expectedKey []byte) ([]byte, error) {
	_, err := cdb.file.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	keyLength, valueLength, err := cdb.readTuple()
	if err != nil {
		return nil, err
	}

	// We can compare key lengths before reading the key at all.
	if int(keyLength) != len(expectedKey) {
		return nil, nil
	}

	buf := make([]byte, keyLength+valueLength)
	_, err = io.ReadFull(cdb.file, buf)
	if err != nil {
		return nil, err
	}

	// If they keys don't match, this isn't it.
	if bytes.Compare(buf[:keyLength], expectedKey) != 0 {
		return nil, nil
	}

	return buf[keyLength:], nil
}

func (cdb *CDB) readTuple() (uint32, uint32, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(cdb.file, buf)
	if err != nil {
		return 0, 0, err
	}

	first := binary.LittleEndian.Uint32(buf[:4])
	second := binary.LittleEndian.Uint32(buf[4:])
	return first, second, nil
}