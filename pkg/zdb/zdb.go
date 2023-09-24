package zdb

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	cl *redis.Client
}

type KeyInfo struct {
	Key       string
	Size      uint64
	Timestamp int64
}

type ScanResponse struct {
	Next string
	Keys []KeyInfo
}

var (
	ErrCursorNoMoreData = errors.New("No more data")
	ErrNil              = redis.Nil
)

func NewClient(address string) Client {
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})

	return Client{
		cl: client,
	}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.cl.Ping(ctx).Err()
}

func (c *Client) Set(ctx context.Context, key, value string) error {
	return c.cl.Do(ctx, "SET", key, value).Err()
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	res, err := c.cl.Do(ctx, "GET", key).Text()
	if err != nil && errors.Is(err, redis.Nil) {
		return "", ErrNil
	}
	if err != nil {
		return "", err
	}

	return res, nil
}

func (c *Client) MGet(ctx context.Context, key []string) ([]string, error) {
	args := make([]interface{}, 0, 1+len(key))
	args = append(args, "MGET")
	for _, arg := range key {
		args = append(args, arg)
	}

	res, err := c.cl.Do(ctx, args...).StringSlice()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	return c.cl.Do(ctx, "DEL", key).Err()
}

func (c *Client) Stop(ctx context.Context) error {
	_, err := c.cl.Do(ctx, "STOP").Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	res, err := c.cl.Do(ctx, "EXISTS", key).Int64()
	if err != nil {
		return false, err
	}

	exists := false
	if res == 1 {
		exists = true
	}

	return exists, nil
}

func (c *Client) Check(ctx context.Context, key string) (bool, error) {
	res, err := c.cl.Do(ctx, "CHECKS", key).Int64()
	if err != nil {
		return false, err
	}

	checks := false
	if res == 1 {
		checks = true
	}

	return checks, nil
}

func (c *Client) KeyCursor(ctx context.Context, key string) (string, error) {
	return c.cl.Do(ctx, "KEYCUR", key).Text()
}

func (c *Client) Info(ctx context.Context) (map[string]string, error) {
	res, err := c.cl.Do(ctx, "INFO").Text()
	if err != nil {
		return nil, err
	}

	return parseInfoResponse(res)
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

func (c *Client) NewNamespace(ctx context.Context, ns string) error {
	return c.cl.Do(ctx, "NSNEW", ns).Err()
}

func (c *Client) DeleteNamespace(ctx context.Context, ns string) error {
	_, err := c.cl.Do(ctx, "NSDEL", ns).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) NamespaceInfo(ctx context.Context, ns string) (string, error) {
	res, err := c.cl.Do(ctx, "NSINFO", ns).Result()
	if err != nil {
		return "", err
	}

	return res.(string), nil
}

func (c *Client) ListNamespaces(ctx context.Context) ([]string, error) {
	res, err := c.cl.Do(ctx, "NSLIST").Result()
	if err != nil {
		return nil, err
	}

	namespaces := res.([]interface{})
	ret := make([]string, 0, len(namespaces))
	for _, ns := range namespaces {
		ret = append(ret, ns.(string))
	}

	return ret, nil
}

func (c *Client) SetNamespace(ctx context.Context, ns string, property string, val string) error {
	_, err := c.cl.Do(ctx, "NSSET", ns, property, val).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) JumpNamespace(ctx context.Context) error {
	_, err := c.cl.Do(ctx, "NSJUMP").Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Select(ctx context.Context, ns string) error {
	_, err := c.cl.Do(ctx, "SELECT", ns).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) SelectSecure(ctx context.Context, ns, password string) error {
	_, err := c.cl.Do(ctx, "SELECT", ns, "SECURE", password).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) GetSize(ctx context.Context) (uint64, error) {
	res, err := c.cl.Do(ctx, "DBSIZE").Result()
	if err != nil {
		return 0, err
	}

	return res.(uint64), nil
}

func (c *Client) Time(ctx context.Context) (int64, error) {
	res, err := c.cl.Do(ctx, "TIME").Result()
	if err != nil {
		return 0, err
	}

	return res.(int64), nil
}

func (c *Client) Auth(ctx context.Context, password string) error {
	_, err := c.cl.Do(ctx, "AUTH", password).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) AuthSecure(ctx context.Context, password string) error {
	res, err := c.cl.Do(ctx, "AUTH SECURE CHALLENGE").Result()
	if err != nil {
		return err
	}

	challenge := res.(string)
	toHash := fmt.Sprintf("%s:%s", challenge, password)

	hash := sha1.New()
	_, _ = hash.Write([]byte(toHash))

	hashStr := string(hash.Sum(nil))

	_, err = c.cl.Do(ctx, "AUTH SECURE", hashStr).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Scan(ctx context.Context) (ScanResponse, error) {
	res, err := c.cl.Do(ctx, "SCAN").Slice()
	if err != nil && err.Error() == ErrCursorNoMoreData.Error() {
		return ScanResponse{}, ErrCursorNoMoreData
	}
	if err != nil {
		return ScanResponse{}, err
	}

	return parseScanResponse(res)
}

func (c *Client) ScanCursor(ctx context.Context, cursor string) (ScanResponse, error) {
	res, err := c.cl.Do(ctx, "SCAN", cursor).Slice()
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
			Key:       key,
			Size:      uint64(size),
			Timestamp: ts,
		}

		ret = append(ret, info)
	}

	return ScanResponse{
		Next: nextCursor,
		Keys: ret,
	}, nil
}

func (c *Client) RScan(ctx context.Context) (ScanResponse, error) {
	res, err := c.cl.Do(ctx, "RSCAN").Slice()
	if err != nil && err.Error() == ErrCursorNoMoreData.Error() {
		return ScanResponse{}, ErrCursorNoMoreData
	}
	if err != nil {
		return ScanResponse{}, err
	}

	return parseScanResponse(res)
}

func (c *Client) RScanCursor(ctx context.Context, cursor string) (ScanResponse, error) {
	res, err := c.cl.Do(ctx, "RSCAN", cursor).Slice()
	if err != nil && err.Error() == ErrCursorNoMoreData.Error() {
		return ScanResponse{}, ErrCursorNoMoreData
	}
	if err != nil {
		return ScanResponse{}, err
	}

	return parseScanResponse(res)
}

func (c *Client) Wait(ctx context.Context, command string) error {
	_, err := c.cl.Do(ctx, "WAIT", command).Result()
	if err != nil {
		return err
	}

	return nil
}
func (c *Client) WaitWithTimeout(ctx context.Context, command string, timeout uint64) error {
	_, err := c.cl.Do(ctx, "WAIT", command, timeout).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) History(ctx context.Context, key string, data string) (string, error) {
	res, err := c.cl.Do(ctx, "HISTORY", key).Result()
	if err != nil {
		return "", err
	}

	return res.(string), nil
}

func (c *Client) HistoryWithData(ctx context.Context, key string, data string) (string, error) {
	res, err := c.cl.Do(ctx, "HISTORY", key, data).Result()
	if err != nil {
		return "", err
	}

	return res.(string), nil
}

func (c *Client) Flush(ctx context.Context) error {
	_, err := c.cl.Do(ctx, "FLUSH").Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Hooks(ctx context.Context) ([]string, error) {
	res, err := c.cl.Do(ctx, "HOOKS").Result()
	if err != nil {
		return nil, err
	}

	hooks := res.([]interface{})
	ret := make([]string, 0, len(hooks))
	for _, hook := range hooks {
		ret = append(ret, hook.(string))
	}

	return ret, nil
}

func (c *Client) IndexDirty(ctx context.Context) ([]string, error) {
	res, err := c.cl.Do(ctx, "INDEX DIRTY").Result()
	if err != nil {
		return nil, err
	}

	ids := res.([]interface{})
	ret := make([]string, 0, len(ids))
	for _, hook := range ids {
		ret = append(ret, hook.(string))
	}

	return ret, nil
}

func (c *Client) IndexDirtyReset(ctx context.Context) error {
	_, err := c.cl.Do(ctx, "INDEX DIRTY RESET").Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) DataRaw(ctx context.Context, field, offset string) ([]string, error) {
	res, err := c.cl.Do(ctx, "DATA RAW", field, offset).Result()
	if err != nil {
		return nil, err
	}

	info := res.([]interface{})
	ret := make([]string, 0, len(info))
	for _, hook := range info {
		ret = append(ret, hook.(string))
	}

	return ret, nil
}

func (c *Client) Length(ctx context.Context, key string) (uint64, error) {
	res, err := c.cl.Do(ctx, "LENGTH", key).Result()
	if err != nil {
		return 0, err
	}

	return res.(uint64), err
}

func (c *Client) KeyTime(ctx context.Context, key string) (int64, error) {
	res, err := c.cl.Do(ctx, "KEYTIME").Result()
	if err != nil {
		return 0, err
	}

	return res.(int64), nil
}

func (c *Client) Close() error {
	return c.cl.Close()
}
