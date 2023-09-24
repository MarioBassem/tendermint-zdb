package zdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZDBSet(t *testing.T) {
	zdb := NewClient("localhost:9900")

	key := "k1"
	want := "v1"
	err := zdb.Set(context.Background(), key, want)
	assert.NoError(t, err)

	got, err := zdb.Get(context.Background(), key)
	assert.NoError(t, err)

	assert.Equal(t, want, got)

	err = zdb.Delete(context.Background(), key)
	assert.NoError(t, err)
}

func TestZDBMultipleGet(t *testing.T) {
	zdb := NewClient("localhost:9900")

	keys := []string{"k1", "k2", "k3"}
	want := []string{"v1", "v2", "v3"}

	for idx := range keys {
		err := zdb.Set(context.Background(), keys[idx], want[idx])
		assert.NoError(t, err)
	}

	got, err := zdb.MGet(context.Background(), keys)
	assert.NoError(t, err)

	assert.Equal(t, want, got)

	for idx := range keys {
		err = zdb.Delete(context.Background(), keys[idx])
		assert.NoError(t, err)
	}
}

func TestExists(t *testing.T) {
	zdb := NewClient("localhost:9900")

	key := "k1"
	val := "v1"
	err := zdb.Set(context.Background(), key, val)
	assert.NoError(t, err)

	want := true
	got, err := zdb.Exists(context.Background(), key)
	assert.NoError(t, err)

	assert.Equal(t, want, got)

	err = zdb.Delete(context.Background(), key)
	assert.NoError(t, err)

	want = false
	got, err = zdb.Exists(context.Background(), key)
	assert.NoError(t, err)

	assert.Equal(t, want, got)
}

func TestCheck(t *testing.T) {
	zdb := NewClient("localhost:9900")

	key := "key2"
	want := "val2"
	err := zdb.Set(context.Background(), key, want)
	assert.NoError(t, err)

	got, err := zdb.Get(context.Background(), key)
	assert.NoError(t, err)

	assert.Equal(t, want, got)

	err = zdb.Delete(context.Background(), key)
	assert.NoError(t, err)
}
