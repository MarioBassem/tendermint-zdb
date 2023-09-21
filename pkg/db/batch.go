package db

import tmdb "github.com/tendermint/tm-db"

type ZDBBatch struct{}

var _ tmdb.Batch = (*ZDBBatch)(nil)

// Set sets a key/value pair.
// CONTRACT: key, value readonly []byte
func (z *ZDBBatch) Set(key, value []byte) error

// Delete deletes a key/value pair.
// CONTRACT: key readonly []byte
func (z *ZDBBatch) Delete(key []byte) error

// Write writes the batch, possibly without flushing to disk. Only Close() can be called after,
// other methods will error.
func (z *ZDBBatch) Write() error

// WriteSync writes the batch and flushes it to disk. Only Close() can be called after, other
// methods will error.
func (z *ZDBBatch) WriteSync() error

// Close closes the batch. It is idempotent, but calls to other methods afterwards will error.
func (z *ZDBBatch) Close() error
