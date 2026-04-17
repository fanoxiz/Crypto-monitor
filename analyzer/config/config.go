package config

import (
	"fmt"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Port              string             `yaml:"port" env:"ANALYZER_PORT" env-default:"8081"`
	Fees              map[string]float64 `yaml:"fees"`
	ExecutorEndpoint  string             `yaml:"executor_url" env:"EXECUTOR_URL" env-default:"http://localhost:8082/deals"`
	HTTPClientTimeout time.Duration      `yaml:"http_client_timeout" env:"ANALYZER_HTTP_CLIENT_TIMEOUT" env-default:"2s"`
}

func Load(path string) (Config, error) {
	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return Config{}, fmt.Errorf("config error: %w", err)
	}

	return cfg, nil
}
