package db

import (
	tmdb "github.com/tendermint/tm-db"
)

var _ tmdb.DB = (*ZDB)(nil)

type ZDB struct {
}

// Get fetches the value of the given key, or nil if it does not exist.
// CONTRACT: key, value readonly []byte
func (z *ZDB) Get([]byte) ([]byte, error)

// Has checks if a key exists.
// CONTRACT: key, value readonly []byte
func (z *ZDB) Has(key []byte) (bool, error)

// Set sets the value for the given key, replacing it if it already exists.
// CONTRACT: key, value readonly []byte
func (z *ZDB) Set([]byte, []byte) error

// SetSync sets the value for the given key, and flushes it to storage before returning.
func (z *ZDB) SetSync([]byte, []byte) error

// Delete deletes the key, or does nothing if the key does not exist.
// CONTRACT: key readonly []byte
func (z *ZDB) Delete([]byte) error

// DeleteSync deletes the key, and flushes the delete to storage before returning.
func (z *ZDB) DeleteSync([]byte) error

// Iterator returns an iterator over a domain of keys, in ascending order. The caller must call
// Close when done. End is exclusive, and start must be less than end. A nil start iterates
// from the first key, and a nil end iterates to the last key (inclusive). Empty keys are not
// valid.
// CONTRACT: No writes may happen within a domain while an iterator exists over it.
// CONTRACT: start, end readonly []byte
func (z *ZDB) Iterator(start, end []byte) (tmdb.Iterator, error)

// ReverseIterator returns an iterator over a domain of keys, in descending order. The caller
// must call Close when done. End is exclusive, and start must be less than end. A nil end
// iterates from the last key (inclusive), and a nil start iterates to the first key (inclusive).
// Empty keys are not valid.
// CONTRACT: No writes may happen within a domain while an iterator exists over it.
// CONTRACT: start, end readonly []byte
func (z *ZDB) ReverseIterator(start, end []byte) (tmdb.Iterator, error)

// Close closes the database connection.
func (z *ZDB) Close() error

// NewBatch creates a batch for atomic updates. The caller must call Batch.Close.
func (z *ZDB) NewBatch() tmdb.Batch

// Print is used for debugging.
func (z *ZDB) Print() error

// Stats returns a map of property values for all keys and the size of the cache.
func (z *ZDB) Stats() map[string]string
