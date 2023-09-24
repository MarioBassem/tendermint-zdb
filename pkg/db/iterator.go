package db

import (
	"context"
	"errors"

	tmdb "github.com/tendermint/tm-db"
)

type zdbIterator struct {
	zdb         *ZDB
	forward     bool
	start       []byte
	end         []byte
	nextCursor  string
	scannedKeys []string
	valid       bool
	err         error
}

var _ tmdb.Iterator = (*zdbIterator)(nil)

// Domain returns the start (inclusive) and end (exclusive) limits of the iterator.
// CONTRACT: start, end readonly []byte
func (z *zdbIterator) Domain() (start []byte, end []byte) {
	return z.start, z.end
}

// Valid returns whether the current iterator is valid. Once invalid, the Iterator remains
// invalid forever.
// Valid returns false whenever the zdb connection is corrupt, or the current key is deleted
func (z *zdbIterator) Valid() bool {
	if !z.valid {
		return false
	}

	if err := z.zdb.cl.Ping(context.TODO()); err != nil {
		z.invalidate(err)
		return false
	}

	exists, err := z.zdb.cl.Exists(context.Background(), z.scannedKeys[0])
	if err != nil {
		z.invalidate(err)
		return false
	}

	return exists
}

// Next moves the iterator to the next key in the database, as defined by order of iteration.
// If Valid returns false, this method will panic.
func (z *zdbIterator) Next() {
	if !z.Valid() {
		panic(z.err)
	}

	z.scannedKeys = z.scannedKeys[1:]

	if len(z.scannedKeys) > 0 {
		return
	}

	scanResponse, err := z.zdb.cl.ScanCursor(context.TODO(), z.nextCursor)
	if err != nil {
		z.invalidate(err)
		return
	}

	z.nextCursor = scanResponse.Next
	keys := make([]string, 0, len(scanResponse.Keys))

	for _, k := range scanResponse.Keys {
		if k.Key == string(z.end) {
			break
		}

		keys = append(keys, k.Key)
	}

	if len(keys) == 0 {
		z.invalidate(errors.New("end of data"))
		return
	}

	z.scannedKeys = keys
}

// Key returns the key at the current position. Panics if the iterator is invalid.
// CONTRACT: key readonly []byte
func (z *zdbIterator) Key() (key []byte) {
	if !z.Valid() {
		panic(z.err)
	}

	return []byte(z.scannedKeys[0])
}

// Value returns the value at the current position. Panics if the iterator is invalid.
// CONTRACT: value readonly []byte
func (z *zdbIterator) Value() (value []byte) {
	if !z.Valid() {
		panic(z.err)
	}

	val, err := z.zdb.cl.Get(context.TODO(), z.scannedKeys[0])
	if err != nil {
		z.invalidate(err)
		panic(z.err)
	}

	return []byte(val)
}

// Error returns the last error encountered by the iterator, if any.
func (z *zdbIterator) Error() error {
	return z.err
}

// Close closes the iterator, relasing any allocated resources.
func (z *zdbIterator) Close() error {
	z.invalidate(errors.New("iterator closed"))
	return nil
}

func (z *zdbIterator) invalidate(err error) {
	z.valid = false
	z.err = err
}
