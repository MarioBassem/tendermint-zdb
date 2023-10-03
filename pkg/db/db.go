package db

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/gomodule/redigo/redis"
	tmdb "github.com/tendermint/tm-db"
)

var _ tmdb.DB = (*ZDB)(nil)

var ErrCursorNoMoreData = errors.New("No more data")
var ErrKeyNotFound = errors.New("Key not found")

type ZDB struct {
	con redis.Conn
}

type ScanResponse struct {
	Next []byte
	Keys []KeyInfo
}

type KeyInfo struct {
	Key       []byte
	Size      uint64
	Timestamp int64
}

func NewZDB(address string) (ZDB, error) {
	con, err := redis.Dial("tcp", address)
	if err != nil {
		return ZDB{}, err
	}

	return ZDB{
		con: con,
	}, nil
}

// Get fetches the value of the given key, or nil if it does not exist.
// CONTRACT: key, value readonly []byte
func (z *ZDB) Get(key []byte) ([]byte, error) {
	res, err := redis.Bytes(z.con.Do("GET", key))
	if err != nil && errors.Is(err, redis.ErrNil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Has checks if a key exists.
// CONTRACT: key, value readonly []byte
func (z *ZDB) Has(key []byte) (bool, error) {
	return redis.Bool(z.con.Do("EXISTS", key))
}

// Set sets the value for the given key, replacing it if it already exists.
// CONTRACT: key, value readonly []byte
func (z *ZDB) Set(key, val []byte) error {
	_, err := z.con.Do("SET", key, val)
	return err
}

// SetSync sets the value for the given key, and flushes it to storage before returning.
func (z *ZDB) SetSync(key, val []byte) error {
	return z.Set(key, val)
}

// Delete deletes the key, or does nothing if the key does not exist.
// CONTRACT: key readonly []byte
func (z *ZDB) Delete(key []byte) error {
	_, err := z.con.Do("DEL", key)
	return err
}

// DeleteSync deletes the key, and flushes the delete to storage before returning.
func (z *ZDB) DeleteSync(key []byte) error {
	return z.Delete(key)
}

// Iterator returns an iterator over a domain of keys, in ascending order. The caller must call
// Close when done. End is exclusive, and start must be less than end. A nil start iterates
// from the first key, and a nil end iterates to the last key (inclusive). Empty keys are not
// valid.
// CONTRACT: No writes may happen within a domain while an iterator exists over it.
// CONTRACT: start, end readonly []byte
func (z *ZDB) Iterator(start, end []byte) (tmdb.Iterator, error) {
	log.Printf("iterator start: %v, end: %v", start, end)

	startCursor, err := z.KeyCursor(start)
	if err != nil && err.Error() == ErrKeyNotFound.Error() {
		return &zdbIterator{
			zdb:     z,
			start:   start,
			end:     end,
			forward: true,
			valid:   false,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get key cursor for %v: %w", start, err)
	}

	scanResponse, err := z.getScanResponse(startCursor)
	if err != nil {
		return nil, fmt.Errorf("failed to get scan result: %w", err)
	}

	keys := make([][]byte, 0, len(scanResponse.Keys)+1)

	keys = append(keys, start)
	for _, k := range scanResponse.Keys {
		if bytes.Equal(k.Key, end) {
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

func (z *ZDB) getScanResponse(start []byte) (ScanResponse, error) {
	if start == nil {
		return z.Scan()
	}

	return z.ScanCursor(start)
}

// ReverseIterator returns an iterator over a domain of keys, in descending order. The caller
// must call Close when done. End is exclusive, and start must be less than end. A nil end
// iterates from the last key (inclusive), and a nil start iterates to the first key (inclusive).
// Empty keys are not valid.
// CONTRACT: No writes may happen within a domain while an iterator exists over it.
// CONTRACT: start, end readonly []byte
func (z *ZDB) ReverseIterator(start, end []byte) (tmdb.Iterator, error) {
	startCursor, err := z.KeyCursor(start)
	if err != nil && err.Error() == ErrKeyNotFound.Error() {
		return &zdbIterator{
			zdb:     z,
			forward: false,
			valid:   false,
			start:   start,
			end:     end,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get key cursor for %v: %w", start, err)
	}

	scanResponse, err := z.getRScanResponse(startCursor)
	if err != nil {
		return nil, fmt.Errorf("failed to get scan result: %w", err)
	}

	keys := make([][]byte, 0, len(scanResponse.Keys)+1)

	keys = append(keys, start)
	for _, k := range scanResponse.Keys {
		if bytes.Equal(k.Key, end) {
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

func (z *ZDB) getRScanResponse(start []byte) (ScanResponse, error) {
	if start == nil {
		return z.ReverseScan()
	}

	return z.ReverseScanCursor(start)
}

// Close closes the database connection.
func (z *ZDB) Close() error {
	return z.con.Close()
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
	res, err := redis.String(z.con.Do("INFO"))
	if err != nil {
		log.Printf("failed to get db info: %s", err.Error())
		return nil
	}

	stats, err := parseInfoResponse(res)
	if err != nil {
		log.Printf("failed to get db info: %s", err.Error())
		return nil
	}

	return stats
}

func parseInfoResponse(res string) (map[string]string, error) {
	slice := strings.Split(res, "\n")
	parsed := make(map[string]string, len(slice))
	for _, s := range slice {
		key, value, found := strings.Cut(s, ":")
		if !found {
			continue
		}

		parsed[key] = strings.Trim(value, " \r\n")
	}

	return parsed, nil
}

func (z *ZDB) Scan() (ScanResponse, error) {
	res, err := redis.Values(z.con.Do("SCAN"))
	if err != nil && err.Error() == ErrCursorNoMoreData.Error() {
		return ScanResponse{}, ErrCursorNoMoreData
	}
	if err != nil {
		return ScanResponse{}, err
	}

	return parseScanResponse(res)
}

func (z *ZDB) ScanCursor(cursor []byte) (ScanResponse, error) {
	res, err := redis.Values(z.con.Do("SCAN", cursor))
	if err != nil && err.Error() == ErrCursorNoMoreData.Error() {
		return ScanResponse{}, ErrCursorNoMoreData
	}
	if err != nil {
		return ScanResponse{}, err
	}

	return parseScanResponse(res)
}

func (z *ZDB) ReverseScan() (ScanResponse, error) {
	res, err := redis.Values(z.con.Do("RSCAN"))
	if err != nil && err.Error() == ErrCursorNoMoreData.Error() {
		return ScanResponse{}, ErrCursorNoMoreData
	}
	if err != nil {
		return ScanResponse{}, err
	}

	return parseScanResponse(res)
}

func (z *ZDB) ReverseScanCursor(cursor []byte) (ScanResponse, error) {
	res, err := redis.Values(z.con.Do("RSCAN", cursor))
	if err != nil && err.Error() == ErrCursorNoMoreData.Error() {
		return ScanResponse{}, ErrCursorNoMoreData
	}
	if err != nil {
		return ScanResponse{}, err
	}

	return parseScanResponse(res)
}

func parseScanResponse(res []interface{}) (ScanResponse, error) {
	if len(res) != 2 {
		return ScanResponse{}, fmt.Errorf("invalid response, scan operations should return two elements, but %d were returned", len(res))
	}

	nextCursor, ok := res[0].(string)
	if !ok {
		return ScanResponse{}, fmt.Errorf("invalid response, expected next key to be string, but a %t was returned", res[0])
	}

	keys, ok := res[1].([]interface{})
	if !ok {
		return ScanResponse{}, fmt.Errorf("invalid response, expected keys to be a slice, but a %t was returned", res[1])
	}

	ret := make([]KeyInfo, 0, len(keys))

	for _, k := range keys {
		x, ok := k.([]interface{})
		if !ok {
			return ScanResponse{}, fmt.Errorf("invalid response, expected key information to be a slice, but a %t was returned", k)
		}

		if len(x) != 3 {
			return ScanResponse{}, fmt.Errorf("invalid response, expected key information to be a slice with 3 elements, but %d elements were returned", len(x))
		}

		key, ok := x[0].(string)
		if !ok {
			return ScanResponse{}, fmt.Errorf("invalid response, expected key to be a string, but a %t was returned", x[0])
		}

		size, ok := x[1].(int64)
		if !ok {
			return ScanResponse{}, fmt.Errorf("invalid response, expected key size to be a int64, but a %t was returned", x[1])
		}

		ts, ok := x[2].(int64)
		if !ok {
			return ScanResponse{}, fmt.Errorf("invalid response, expected key creation timestamp to be an int64, but a %t was returned", x[2])
		}

		info := KeyInfo{
			Key:       []byte(key),
			Size:      uint64(size),
			Timestamp: ts,
		}

		ret = append(ret, info)
	}

	return ScanResponse{
		Next: []byte(nextCursor),
		Keys: ret,
	}, nil
}

func (z *ZDB) KeyCursor(key []byte) ([]byte, error) {
	return redis.Bytes(z.con.Do("KEYCUR", key))
}

func (z *ZDB) Ping() error {
	_, err := z.con.Do("PING")
	if err != nil {
		return err
	}

	return nil
}

func (z *ZDB) Exists(key []byte) (bool, error) {
	return redis.Bool(z.con.Do("EXISTS", key))
}

func (z *ZDB) NewNamespace(ns string) error {
	_, err := z.con.Do("NSNEW", ns)
	return err
}

func (z *ZDB) Select(ns string) error {
	_, err := z.con.Do("SELECT", ns)
	return err
}

func (z *ZDB) DeleteNamespace(ns string) error {
	_, err := z.con.Do("DELETE", ns)
	return err
}
