package db

import (
	"context"
	"errors"
	"fmt"
	"log"

	tmdb "github.com/tendermint/tm-db"
	"github.com/threefoldtech/tendermint-zdb/pkg/zdb"
)

var _ tmdb.DB = (*ZDB)(nil)

type ZDB struct {
	cl *zdb.Client
}

func NewZDB(cl *zdb.Client) ZDB {
	return ZDB{
		cl: cl,
	}
}

// Get fetches the value of the given key, or nil if it does not exist.
// CONTRACT: key, value readonly []byte
func (z *ZDB) Get(key []byte) ([]byte, error) {
	res, err := z.cl.Get(context.TODO(), string(key))
	if err != nil && errors.Is(err, zdb.ErrNil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return []byte(res), nil
}

// Has checks if a key exists.
// CONTRACT: key, value readonly []byte
func (z *ZDB) Has(key []byte) (bool, error) {
	return z.cl.Exists(context.TODO(), string(key))
}

// Set sets the value for the given key, replacing it if it already exists.
// CONTRACT: key, value readonly []byte
func (z *ZDB) Set(key, val []byte) error {
	return z.cl.Set(context.TODO(), string(key), string(val))
}

// SetSync sets the value for the given key, and flushes it to storage before returning.
func (z *ZDB) SetSync(key, val []byte) error {
	return z.cl.Set(context.TODO(), string(key), string(val))
}

// Delete deletes the key, or does nothing if the key does not exist.
// CONTRACT: key readonly []byte
func (z *ZDB) Delete(key []byte) error {
	return z.cl.Delete(context.TODO(), string(key))
}

// DeleteSync deletes the key, and flushes the delete to storage before returning.
func (z *ZDB) DeleteSync(key []byte) error {
	return z.cl.Delete(context.TODO(), string(key))
}

// Iterator returns an iterator over a domain of keys, in ascending order. The caller must call
// Close when done. End is exclusive, and start must be less than end. A nil start iterates
// from the first key, and a nil end iterates to the last key (inclusive). Empty keys are not
// valid.
// CONTRACT: No writes may happen within a domain while an iterator exists over it.
// CONTRACT: start, end readonly []byte
func (z *ZDB) Iterator(start, end []byte) (tmdb.Iterator, error) {
	scanResponse, err := z.getScanResponse(start)
	if err != nil {
		return nil, fmt.Errorf("failed to get scan result: %w", err)
	}

	keys := make([]string, 0, len(scanResponse.Keys)+1)

	keys = append(keys, string(start))
	for _, k := range scanResponse.Keys {
		if k.Key == string(end) {
			break
		}

		keys = append(keys, k.Key)
	}

	iterator := zdbIterator{
		zdb:         z,
		start:       start,
		end:         end,
		nextCursor:  scanResponse.Next,
		scannedKeys: keys,
		forward:     true,
		valid:       true,
	}

	return &iterator, nil
}

func (z *ZDB) getScanResponse(start []byte) (zdb.ScanResponse, error) {
	if start == nil {
		return z.cl.Scan(context.TODO())
	}

	startCursor, err := z.cl.KeyCursor(context.TODO(), string(start))
	if err != nil {
		return zdb.ScanResponse{}, fmt.Errorf("failed to get key cursor for %s: %w", start, err)
	}

	return z.cl.ScanCursor(context.TODO(), startCursor)
}

// ReverseIterator returns an iterator over a domain of keys, in descending order. The caller
// must call Close when done. End is exclusive, and start must be less than end. A nil end
// iterates from the last key (inclusive), and a nil start iterates to the first key (inclusive).
// Empty keys are not valid.
// CONTRACT: No writes may happen within a domain while an iterator exists over it.
// CONTRACT: start, end readonly []byte
func (z *ZDB) ReverseIterator(start, end []byte) (tmdb.Iterator, error) {
	scanResponse, err := z.getRScanResponse(start)
	if err != nil {
		return nil, fmt.Errorf("failed to get scan result: %w", err)
	}

	keys := make([]string, 0, len(scanResponse.Keys)+1)

	keys = append(keys, string(start))
	for _, k := range scanResponse.Keys {
		if k.Key == string(end) {
			break
		}

		keys = append(keys, k.Key)
	}

	iterator := zdbIterator{
		zdb:         z,
		start:       start,
		end:         end,
		nextCursor:  scanResponse.Next,
		scannedKeys: keys,
		forward:     false,
		valid:       true,
	}

	return &iterator, nil
}

func (z *ZDB) getRScanResponse(start []byte) (zdb.ScanResponse, error) {
	if start == nil {
		return z.cl.RScan(context.Background())
	}

	startCursor, err := z.cl.KeyCursor(context.TODO(), string(start))
	if err != nil {
		return zdb.ScanResponse{}, fmt.Errorf("failed to get key cursor for %s: %w", start, err)
	}

	return z.cl.RScanCursor(context.TODO(), startCursor)
}

// Close closes the database connection.
func (z *ZDB) Close() error {
	return z.cl.Close()
}

// NewBatch creates a batch for atomic updates. The caller must call Batch.Close.
func (z *ZDB) NewBatch() tmdb.Batch {
	return &ZDBBatch{
		zdb:     z,
		setOps:  make([]Op, 0),
		delKeys: make([][]byte, 0),
		closed:  false,
	}
}

// Print is used for debugging.
func (z *ZDB) Print() error {
	return nil
}

// Stats returns a map of property values for all keys and the size of the cache.
func (z *ZDB) Stats() map[string]string {
	info, err := z.cl.Info(context.TODO())
	if err != nil {
		log.Printf("failed to get db info: %s", err.Error())
		return nil
	}

	return info
}
