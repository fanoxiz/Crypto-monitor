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
binance_ws_url: wss://binance.example/ws
bybit_ws_url: wss://bybit.example/ws
bitget_ws_url: wss://bitget.example/ws
okx_ws_url: wss://okx.example/ws
`

func TestLoadDefaultsAndNormalizeCoins(t *testing.T) {
	testsupport.ResetEnv(t)

	cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", validConfig))
	if err != nil {
		t.Fatal(err)
	}

	want := Config{
		TrackedCoins: []string{"BTC", "ETH"},
		RedisAddr:    "localhost:6379",
		PriceTTL:     600 * time.Millisecond,
		BinanceURL:   "wss://binance.example/ws",
		BybitURL:     "wss://bybit.example/ws",
		BitgetURL:    "wss://bitget.example/ws",
		OKXURL:       "wss://okx.example/ws",
	}

	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("configuration = %#v, want %#v", cfg, want)
	}
}

func TestLoadYAMLValuesWithoutEnvironment(t *testing.T) {
	testsupport.ResetEnv(t)

	contents := validConfig + "redis_addr: 127.0.0.1:16379\nprice_ttl: 1750ms\n"
	cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", contents))
	if err != nil {
		t.Fatal(err)
	}

	want := Config{
		TrackedCoins: []string{"BTC", "ETH"},
		RedisAddr:    "127.0.0.1:16379",
		PriceTTL:     1750 * time.Millisecond,
		BinanceURL:   "wss://binance.example/ws",
		BybitURL:     "wss://bybit.example/ws",
		BitgetURL:    "wss://bitget.example/ws",
		OKXURL:       "wss://okx.example/ws",
	}

	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("YAML configuration = %#v, want %#v", cfg, want)
	}
}

func TestLoadEnvironmentOverridesYAML(t *testing.T) {
	testsupport.ResetEnv(t)

	values := map[string]string{
		"TRACKED_COINS":     " sol , xaut ",
		"REDIS_ADDR":        "127.0.0.1:16379",
		"FETCHER_PRICE_TTL": "1750ms",
		"BINANCE_WS_URL":    "ws://127.0.0.1:9001/binance",
		"BYBIT_WS_URL":      "ws://127.0.0.1:9002/bybit",
		"BITGET_WS_URL":     "ws://127.0.0.1:9003/bitget",
		"OKX_WS_URL":        "ws://127.0.0.1:9004/okx",
	}

	for key, value := range values {
		t.Setenv(key, value)
	}

	cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", validConfig+"redis_addr: yaml:6379\nprice_ttl: 9s\n"))
	if err != nil {
		t.Fatal(err)
	}

	want := Config{
		TrackedCoins: []string{"SOL", "XAUT"},
		RedisAddr:    values["REDIS_ADDR"],
		PriceTTL:     1750 * time.Millisecond,
		BinanceURL:   values["BINANCE_WS_URL"],
		BybitURL:     values["BYBIT_WS_URL"],
		BitgetURL:    values["BITGET_WS_URL"],
		OKXURL:       values["OKX_WS_URL"],
	}

	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("configuration = %#v, want %#v", cfg, want)
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
			name: "wrong_coin_type",
			yaml: strings.Replace(validConfig, `[" btc ", "eTh"]`, "{}", 1),
		},
		{
			name: "missing_coins",
			yaml: strings.Replace(validConfig, `tracked_coins: [" btc ", "eTh"]`, "tracked_coins: []", 1),
		},
		{
			name: "negative_ttl",
			yaml: validConfig + "price_ttl: -1ms\n",
		},
		{
			name: "invalid_ttl",
			yaml: validConfig + "price_ttl: broken\n",
		},
		{
			name: "zero_environment_ttl",
			yaml: validConfig,
			env: map[string]string{
				"FETCHER_PRICE_TTL": "0s",
			},
		},
		{
			name: "overflow_environment_ttl",
			yaml: validConfig,
			env: map[string]string{
				"FETCHER_PRICE_TTL": "999999999999h",
			},
		},
	}

	for _, endpoint := range []string{"binance", "bybit", "bitget", "okx"} {
		cases = append(cases, struct {
			name string
			yaml string
			env  map[string]string
		}{
			name: "missing_" + endpoint + "_url",
			yaml: strings.Replace(validConfig, endpoint+"_ws_url: wss://"+endpoint+".example/ws", endpoint+`_ws_url: ""`, 1),
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
