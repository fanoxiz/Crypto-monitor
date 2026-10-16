package cache

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/internal/testsupport"
	"github.com/redis/go-redis/v9"
)

func TestSaveBatchDoesNotPublishFailedMutation(t *testing.T) {
	for _, operation := range []string{"set", "del"} {
		t.Run(operation, func(t *testing.T) {
			client := testsupport.RedisClient(t, 4)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			username := "quote-writer-" + rand.Text()
			password := rand.Text()

			if err := client.Do(ctx, "ACL", "SETUSER", username, "on", ">"+password, "~*", "&*", "+@all", "-"+operation).Err(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := client.Do(context.Background(), "ACL", "DELUSER", username).Err(); err != nil {
					t.Error(err)
				}
			})

			writer := redis.NewClient(&redis.Options{
				Addr:     client.Options().Addr,
				DB:       client.Options().DB,
				Username: username,
				Password: password,
			})
			t.Cleanup(func() {
				writer.Close()
			})

			pubsub := client.Subscribe(ctx, contracts.QuotesChannel)
			defer pubsub.Close()

			stop := context.AfterFunc(ctx, func() {
				pubsub.Close()
			})
			defer stop()

			if _, err := pubsub.Receive(ctx); err != nil {
				t.Fatal(err)
			}

			now := time.Now().UTC().Add(-time.Millisecond)
			quote := contracts.Quote{
				Coin:        "WRITERTEST",
				Exchange:    "Bybit",
				BidPrice:    "99",
				BidSize:     "1",
				AskPrice:    "100",
				AskSize:     "1",
				Invalidated: operation == "del",
				SourceAt:    now,
				ReceivedAt:  now,
			}
			key := contracts.QuoteKey(quote.Coin, quote.Exchange)

			if err := client.Set(ctx, key, "previous", 0).Err(); err != nil {
				t.Fatal(err)
			}

			if err := NewRedisQuoteStore(writer, 10*time.Second).SaveBatch(ctx, []contracts.Quote{quote}); err == nil {
				t.Fatal("denied Redis mutation reported success")
			}

			if value, err := client.Get(ctx, key).Result(); err != nil || value != "previous" {
				t.Fatalf("failed mutation changed the stored value: %q, %v", value, err)
			}

			marker := `{"snapshot":"` + rand.Text() + `"}`

			if err := client.Publish(ctx, contracts.QuotesChannel, marker).Err(); err != nil {
				t.Fatal(err)
			}

			for {
				message, err := pubsub.ReceiveMessage(ctx)
				if err != nil {
					t.Fatal(err)
				}

				if message.Payload == marker {
					return
				}

				var event contracts.Quote

				if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
					t.Fatal(err)
				}

				if event.Coin == quote.Coin {
					t.Fatalf("failed mutation published a quote event: %#v", event)
				}
			}
		})
	}
}
