package zdb

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	cl *redis.Client
}

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

func (c *Client) Set(ctx context.Context, key string, value string) error {
	_, err := c.cl.Do(ctx, "SET", key, value).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Get(ctx context.Context, key string) ([]string, error) {
	res, err := c.cl.Do(ctx, "GET", key).Result()
	if err != nil {
		return nil, err
	}

	return []string{res.(string)}, nil
}

// func (c *Client) Delete(ctx context.Context, key string) error
// func (c *Client) Stop(ctx context.Context) error
// func (c *Client) Exists(ctx context.Context, key string) (bool, error)
// func (c *Client) Check(ctx context.Context, key string) error
// func (c *Client) KeyCursor(ctx context.Context, key string) error
// func (c *Client) Info(ctx context.Context) error
// func (c *Client) NewNamespace(ctx context.Context, ns string) error
// func (c *Client) DeleteNamespace(ctx context.Context, ns string) error
// func (c *Client) NamespaceInfo(ctx context.Context, ns string) error
// func (c *Client) ListNamespaces(ctx context.Context) ([]string, error)
// func (c *Client) SetNamespace(ctx context.Context, ns string, property string, val string) error
// func (c *Client) JumpNamespace(ctx context.Context) error
// func (c *Client) Select(ctx context.Context, ns string) error
// func (c *Client) GetSize(ctx context.Context) (uint64, error)
// func (c *Client) Time(ctx context.Context) (uint64, error)
// func (c *Client) Auth(ctx context.Context, password string) error
// func (c *Client) AuthSecure(ctx context.Context, password string) error
// func (c *Client) Scan(ctx context.Context, cursor string) error
// func (c *Client) RScan(ctx context.Context, cursor string) error
// func (c *Client) Wait(ctx context.Context, command string, timeout uint64) error
// func (c *Client) History(ctx context.Context, key string, data string) error
// func (c *Client) Flush(ctx context.Context) error
// func (c *Client) Hooks(ctx context.Context) error
// func (c *Client) IndexDirty(ctx context.Context) error
// func (c *Client) DataRaw(ctx context.Context) error
// func (c *Client) Length(ctx context.Context, key string) error
// func (c *Client) KeyTime(ctx context.Context, key string) (int64, error)
