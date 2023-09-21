package zdb

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZDB(t *testing.T) {
	zdb := NewClient("localhost:9900")

	err := zdb.Ping(context.Background())
	assert.NoError(t, err)

	// err = zdb.Set(context.Background(), "mario", "yel3ab")
	// assert.NoError(t, err)

	vals, err := zdb.Get(context.Background(), "hamada")
	assert.NoError(t, err)

	log.Print(vals)

	// assert.Equal(t, "yel3ab", vals[0])
}
