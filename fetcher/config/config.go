package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	TrackedCoins []string      `yaml:"tracked_coins" env:"TRACKED_COINS"`
	RedisAddr    string        `yaml:"redis_addr" env:"REDIS_ADDR" env-default:"localhost:6379"`
	PriceTTL     time.Duration `yaml:"price_ttl" env:"FETCHER_PRICE_TTL" env-default:"500ms"`
	BinanceURL   string        `yaml:"binance_ws_url" env:"BINANCE_WS_URL"`
	BybitURL     string        `yaml:"bybit_ws_url" env:"BYBIT_WS_URL"`
	BitgetURL    string        `yaml:"bitget_ws_url" env:"BITGET_WS_URL"`
	OKXURL       string        `yaml:"okx_ws_url" env:"OKX_WS_URL"`
}

func Load(path string) (Config, error) {
	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return Config{}, err
	}

	for i, coin := range cfg.TrackedCoins {
		cfg.TrackedCoins[i] = strings.ToUpper(strings.TrimSpace(coin))
	}

	if len(cfg.TrackedCoins) == 0 {
		return Config{}, fmt.Errorf("tracked_coins must not be empty")
	}

	if cfg.PriceTTL <= 0 {
		return Config{}, fmt.Errorf("price_ttl must be positive")
	}

	if cfg.BinanceURL == "" || cfg.BybitURL == "" || cfg.BitgetURL == "" || cfg.OKXURL == "" {
		return Config{}, fmt.Errorf("WebSocket URLs must not be empty")
	}

	return cfg, nil
}
