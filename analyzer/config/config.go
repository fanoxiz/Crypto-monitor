package config

import (
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Port string             `yaml:"port" env:"ANALYZER_PORT" env-default:"8081"`
	Fees map[string]float64 `yaml:"fees"`
}

func Load(path string) (Config, error) {
	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return Config{}, fmt.Errorf("config error: %w", err)
	}

	return cfg, nil
}
