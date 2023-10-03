package db

import (
	"errors"
	"fmt"
	"sync"

	tmdb "github.com/tendermint/tm-db"
)

type ZDBBatch struct {
	zdb     *ZDB
	setOps  []Op
	delKeys [][]byte
	closed  bool

	sync.Mutex
}

type Op struct {
	key []byte
	val []byte
}

var (
	ErrBatchClosed = errors.New("batch is closed")
)

var _ tmdb.Batch = (*ZDBBatch)(nil)

// Set sets a key/value pair.
// CONTRACT: key, value readonly []byte
func (z *ZDBBatch) Set(key, value []byte) error {
	z.Lock()
	defer z.Unlock()

	if z.closed {
		return ErrBatchClosed
	}

	if key == nil {
		return errors.New("key cannot be nil")
	}

	if value == nil {
		return errors.New("value cannot be nil")
	}

	z.setOps = append(z.setOps, Op{key: key, val: value})

	return nil
}

// Delete deletes a key/value pair.
// CONTRACT: key readonly []byte
func (z *ZDBBatch) Delete(key []byte) error {
	z.Lock()
	defer z.Unlock()

	if z.closed {
		return ErrBatchClosed
	}

	if key == nil {
		return errors.New("key cannot be nil")
	}

	z.delKeys = append(z.delKeys, key)

	return nil
}

// Write implements Batch. since ZDB don't support batch operations, it iteratively tries to execute writes.
func (z *ZDBBatch) Write() error {
	z.Lock()
	defer z.Unlock()

	if z.closed {
		return ErrBatchClosed
	}

	for len(z.setOps) > 0 {
		op := z.setOps[0]
		if err := z.zdb.Set(op.key, op.val); err != nil {
			return fmt.Errorf("batch write failed; try again")
		}

		z.setOps = z.setOps[1:]
	}

	for len(z.delKeys) > 0 {
		key := z.delKeys[0]
		if err := z.zdb.Delete(key); err != nil {
			return fmt.Errorf("batch write failed; try again")
		}

		z.delKeys = z.delKeys[1:]
	}

	return nil
}

// WriteSync writes the batch and flushes it to disk. Only Close() can be called after, other
// methods will error.
func (z *ZDBBatch) WriteSync() error {
	return z.Write()
}

// Close closes the batch. It is idempotent, but calls to other methods afterwards will error.
func (z *ZDBBatch) Close() error {
	z.Lock()
	defer z.Unlock()

	z.closed = true
	return nil
}
