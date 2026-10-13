package testsupport

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func RedisClient(t *testing.T, database int) *redis.Client {
	t.Helper()

	address := os.Getenv("TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("requires an isolated Redis instance in TEST_REDIS_ADDR")
	}

	client := redis.NewClient(&redis.Options{
		Addr: address,
		DB:   database,
	})
	t.Cleanup(func() {
		client.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis test fixture: %v", err)
	}

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	return client
}
