package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	TrackedCoins      []string      `mapstructure:"tracked_coins"`
	RequestFrequency  time.Duration `mapstructure:"request_frequency"`
	AnalyzerEndpoint  string        `mapstructure:"analyzer_url"`
	HTTPClientTimeout time.Duration `mapstructure:"http_client_timeout"`
}

func Load(path string) (Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("error reading config: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("error parsing config: %w", err)
	}

	if len(cfg.TrackedCoins) == 0 {
		return Config{}, fmt.Errorf("tracked_coins is empty")
	}
	if cfg.AnalyzerEndpoint == "" {
		return Config{}, fmt.Errorf("analyzer_url is empty")
	}

	return cfg, nil
}
