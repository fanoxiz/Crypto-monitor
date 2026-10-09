package cache

import (
	"context"
	"encoding/json"
	"log"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/redis/go-redis/v9"
)

type RedisQuoteStore struct {
	client  *redis.Client
	keys    []string
	allowed map[string]bool
}

func NewRedisQuoteStore(client *redis.Client, coins, exchanges []string) *RedisQuoteStore {
	keys := make([]string, 0, len(coins)*len(exchanges))
	allowed := make(map[string]bool, cap(keys))

	for _, coin := range coins {
		for _, exchange := range exchanges {
			key := contracts.QuoteKey(coin, exchange)
			keys = append(keys, key)
			allowed[key] = true
		}
	}

	return &RedisQuoteStore{
		client:  client,
		keys:    keys,
		allowed: allowed,
	}
}

func (s *RedisQuoteStore) Stream(ctx context.Context, handle func(contracts.Quote)) error {
	pubsub := s.client.Subscribe(ctx, contracts.QuotesChannel)
	defer pubsub.Close()

	stop := context.AfterFunc(ctx, func() {
		pubsub.Close()
	})
	defer stop()

	for {
		message, err := pubsub.Receive(ctx)
		if err != nil {
			return err
		}

		switch message := message.(type) {
		case *redis.Subscription:
			if message.Kind != "subscribe" || message.Channel != contracts.QuotesChannel {
				continue
			}

			quotes, err := s.snapshot(ctx)
			if err != nil {
				return err
			}

			log.Printf("level=INFO component=cache event=quotes_subscribed snapshot_size=%d", len(quotes))

			for _, quote := range quotes {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				handle(quote)
			}
		case *redis.Message:
			quote, valid := s.decode(message.Payload)
			if valid {
				handle(quote)
			}
		}
	}
}

func (s *RedisQuoteStore) snapshot(ctx context.Context) ([]contracts.Quote, error) {
	values, err := s.client.MGet(ctx, s.keys...).Result()
	if err != nil {
		return nil, err
	}

	quotes := make([]contracts.Quote, 0, len(values))

	for i, value := range values {
		raw, exists := value.(string)
		if !exists {
			continue
		}

		quote, valid := s.decode(raw)
		if !valid || contracts.QuoteKey(quote.Coin, quote.Exchange) != s.keys[i] {
			continue
		}

		quotes = append(quotes, quote)
	}

	return quotes, nil
}

func (s *RedisQuoteStore) decode(raw string) (contracts.Quote, bool) {
	var quote contracts.Quote

	if err := json.Unmarshal([]byte(raw), &quote); err != nil {
		return contracts.Quote{}, false
	}

	return quote, s.allowed[contracts.QuoteKey(quote.Coin, quote.Exchange)]
}
