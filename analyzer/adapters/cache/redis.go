package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/redis/go-redis/v9"
)

type priceEntry struct {
	Price     contracts.BidAsk `json:"price"`
	UpdatedAt time.Time        `json:"updated_at"`
}

type RedisPriceStore struct {
	client    *redis.Client
	keyPrefix string
	priceTTL  time.Duration
}

func NewRedisPriceStore(client *redis.Client, keyPrefix string, priceTTL time.Duration) *RedisPriceStore {
	return &RedisPriceStore{
		client:    client,
		keyPrefix: keyPrefix,
		priceTTL:  priceTTL,
	}
}

func (s *RedisPriceStore) SetPrice(coin, exchange string, price contracts.BidAsk) error {
	entry := priceEntry{
		Price:     price,
		UpdatedAt: time.Now().UTC(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("redis set price: marshal: %w", err)
	}

	if err := s.client.HSet(context.Background(), s.coinKey(coin), exchange, data).Err(); err != nil {
		return fmt.Errorf("redis set price: hset: %w", err)
	}

	return nil
}

func (s *RedisPriceStore) GetPrices(coin string) (map[string]contracts.BidAsk, error) {
	rows, err := s.client.HGetAll(context.Background(), s.coinKey(coin)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis get prices: hgetall: %w", err)
	}

	now := time.Now().UTC()
	prices := make(map[string]contracts.BidAsk, len(rows))
	var staleFields []string

	for exchange, raw := range rows {
		var entry priceEntry
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			staleFields = append(staleFields, exchange)
			continue
		}

		if s.priceTTL > 0 && now.Sub(entry.UpdatedAt) > s.priceTTL {
			staleFields = append(staleFields, exchange)
			continue
		}

		prices[exchange] = entry.Price
	}

	if len(staleFields) > 0 {
		_ = s.client.HDel(context.Background(), s.coinKey(coin), staleFields...).Err()
	}

	return prices, nil
}

func (s *RedisPriceStore) coinKey(coin string) string {
	if s.keyPrefix == "" {
		return "analyzer:prices:" + coin
	}

	return s.keyPrefix + coin
}
