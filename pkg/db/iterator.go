package db

import tmdb "github.com/tendermint/tm-db"

type ZDBI struct{}

var _ tmdb.Iterator = (*ZDBI)(nil)

// Domain returns the start (inclusive) and end (exclusive) limits of the iterator.
// CONTRACT: start, end readonly []byte
func (z *ZDBI) Domain() (start []byte, end []byte)

// Valid returns whether the current iterator is valid. Once invalid, the Iterator remains
// invalid forever.
func (z *ZDBI) Valid() bool

// Next moves the iterator to the next key in the database, as defined by order of iteration.
// If Valid returns false, this method will panic.
func (z *ZDBI) Next()

// Key returns the key at the current position. Panics if the iterator is invalid.
// CONTRACT: key readonly []byte
func (z *ZDBI) Key() (key []byte)

// Value returns the value at the current position. Panics if the iterator is invalid.
// CONTRACT: value readonly []byte
func (z *ZDBI) Value() (value []byte)

// Error returns the last error encountered by the iterator, if any.
func (z *ZDBI) Error() error

// Close closes the iterator, relasing any allocated resources.
func (z *ZDBI) Close() error
