package config

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/internal/testsupport"
)

const validConfig = `tracked_coins: [" btc ", "eTh"]
exchanges: [Binance, Bybit]
fees:
  Binance: 0
  Bybit: 0.001
  Coinbase: 0.009
`

func TestLoadDefaultsAndNormalizeCoins(t *testing.T) {
	testsupport.ResetEnv(t)

	cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", validConfig))
	if err != nil {
		t.Fatal(err)
	}

	want := Config{
		TrackedCoins: []string{"BTC", "ETH"},
		Exchanges:    []string{"Binance", "Bybit"},
		RedisAddr:    "localhost:6379",
		MaxQuoteAge:  600 * time.Millisecond,
		Fees: map[string]float64{
			"Binance":  0,
			"Bybit":    0.001,
			"Coinbase": 0.009,
		},
		ExecutorGRPCAddr: "localhost:8082",
	}

	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("configuration = %#v, want %#v", cfg, want)
	}
}

func TestLoadYAMLValuesWithoutEnvironment(t *testing.T) {
	testsupport.ResetEnv(t)

	contents := validConfig + "redis_addr: 127.0.0.1:16379\nexecutor_grpc_addr: 127.0.0.1:18082\nmax_quote_age: 1500ms\n"
	cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", contents))
	if err != nil {
		t.Fatal(err)
	}

	want := Config{
		TrackedCoins: []string{"BTC", "ETH"},
		Exchanges:    []string{"Binance", "Bybit"},
		RedisAddr:    "127.0.0.1:16379",
		MaxQuoteAge:  1500 * time.Millisecond,
		Fees: map[string]float64{
			"Binance":  0,
			"Bybit":    0.001,
			"Coinbase": 0.009,
		},
		ExecutorGRPCAddr: "127.0.0.1:18082",
	}

	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("YAML configuration = %#v, want %#v", cfg, want)
	}
}

func TestLoadEnvironmentOverridesYAML(t *testing.T) {
	testsupport.ResetEnv(t)
	t.Setenv("TRACKED_COINS", " sol , xaut ")
	t.Setenv("REDIS_ADDR", "127.0.0.1:16379")
	t.Setenv("EXECUTOR_GRPC_ADDR", "127.0.0.1:18082")
	t.Setenv("ANALYZER_MAX_QUOTE_AGE", "1500ms")

	contents := validConfig + "redis_addr: yaml:6379\nexecutor_grpc_addr: yaml:8082\nmax_quote_age: 9s\n"
	cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", contents))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(cfg.TrackedCoins, []string{"SOL", "XAUT"}) || cfg.RedisAddr != "127.0.0.1:16379" || cfg.ExecutorGRPCAddr != "127.0.0.1:18082" || cfg.MaxQuoteAge != 1500*time.Millisecond {
		t.Fatalf("environment overrides not applied: %#v", cfg)
	}

	if !reflect.DeepEqual(cfg.Exchanges, []string{"Binance", "Bybit"}) || len(cfg.Fees) != 3 || cfg.Fees["Coinbase"] != 0.009 {
		t.Fatalf("additional fee enabled or altered an exchange: %#v", cfg)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		env  map[string]string
	}{
		{
			name: "broken_yaml",
			yaml: "tracked_coins: [",
		},
		{
			name: "wrong_fee_type",
			yaml: "tracked_coins: [BTC]\nexchanges: [Binance, Bybit]\nfees: []\n",
		},
		{
			name: "empty_coins",
			yaml: strings.Replace(validConfig, `[" btc ", "eTh"]`, "[]", 1),
		},
		{
			name: "blank_coin",
			yaml: strings.Replace(validConfig, `[" btc ", "eTh"]`, `["BTC", " "]`, 1),
		},
		{
			name: "normalized_duplicate_coin",
			yaml: strings.Replace(validConfig, `[" btc ", "eTh"]`, `["BTC", " btc "]`, 1),
		},
		{
			name: "too_few_exchanges",
			yaml: strings.Replace(validConfig, "[Binance, Bybit]", "[Binance]", 1),
		},
		{
			name: "duplicate_exchange",
			yaml: strings.Replace(validConfig, "[Binance, Bybit]", "[Binance, Binance]", 1),
		},
		{
			name: "empty_exchange",
			yaml: strings.Replace(validConfig, "[Binance, Bybit]", `[Binance, ""]`, 1),
		},
		{
			name: "missing_active_fee",
			yaml: strings.Replace(validConfig, "  Binance: 0\n", "", 1),
		},
		{
			name: "negative_age",
			yaml: validConfig + "max_quote_age: -1ms\n",
		},
		{
			name: "zero_environment_age",
			yaml: validConfig,
			env: map[string]string{
				"ANALYZER_MAX_QUOTE_AGE": "0s",
			},
		},
		{
			name: "invalid_environment_age",
			yaml: validConfig,
			env: map[string]string{
				"ANALYZER_MAX_QUOTE_AGE": "not-a-duration",
			},
		},
	}

	for _, fee := range []string{"-0.01", "1", "1.1", ".nan", ".inf", "-.inf", "invalid"} {
		cases = append(cases, struct {
			name string
			yaml string
			env  map[string]string
		}{
			name: "invalid_fee_" + fee,
			yaml: strings.Replace(validConfig, "  Binance: 0", "  Binance: "+fee, 1),
		})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testsupport.ResetEnv(t)

			for key, value := range tc.env {
				t.Setenv(key, value)
			}

			cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", tc.yaml))
			if err == nil {
				t.Fatalf("invalid configuration accepted: %#v", cfg)
			}

			if !reflect.DeepEqual(cfg, Config{}) {
				t.Fatalf("error exposed partially loaded configuration: %#v", cfg)
			}
		})
	}
}

func TestLoadRejectsMissingFile(t *testing.T) {
	testsupport.ResetEnv(t)

	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("missing configuration accepted")
	}
}
