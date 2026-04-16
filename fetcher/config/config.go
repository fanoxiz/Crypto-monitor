package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	TrackedCoins      []string      `yaml:"tracked_coins" env:"TRACKED_COINS" env-required:"true"`
	RequestFrequency  time.Duration `yaml:"request_frequency" env:"REQUEST_FREQUENCY" env-default:"1s"`
	AnalyzerEndpoint  string        `yaml:"analyzer_url" env:"ANALYZER_URL" env-required:"true"`
	HTTPClientTimeout time.Duration `yaml:"http_client_timeout" env:"HTTP_CLIENT_TIMEOUT" env-default:"2s"`
}

func Load(path string) (Config, error) {
	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return Config{}, fmt.Errorf("config error: %w", err)
	}

	for i, coin := range cfg.TrackedCoins {
		cfg.TrackedCoins[i] = strings.ToUpper(strings.TrimSpace(coin))
	}

	return cfg, nil
}
