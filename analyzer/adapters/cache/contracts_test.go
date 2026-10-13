package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/internal/testsupport"
)

func TestQuoteContractRestrictsConfiguredPairs(t *testing.T) {
	store := NewRedisQuoteStore(nil, []string{"ETH", "BTC"}, []string{"Bybit", "OKX"})
	wantKeys := []string{
		"fetcher:quotes:spot:ETH-USDT:Bybit",
		"fetcher:quotes:spot:ETH-USDT:OKX",
		"fetcher:quotes:spot:BTC-USDT:Bybit",
		"fetcher:quotes:spot:BTC-USDT:OKX",
	}
	if !reflect.DeepEqual(store.keys, wantKeys) {
		t.Fatalf("configured quote keys = %v, want %v", store.keys, wantKeys)
	}

	for _, fixture := range []struct {
		coin     string
		exchange string
		allowed  bool
	}{
		{
			coin:     "ETH",
			exchange: "Bybit",
			allowed:  true,
		},
		{
			coin:     "BTC",
			exchange: "OKX",
			allowed:  true,
		},
		{
			coin:     "ETH",
			exchange: "Coinbase",
		},
		{
			coin:     "DOGE",
			exchange: "Bybit",
		},
		{
			exchange: "Bybit",
		},
		{
			coin: "ETH",
		},
	} {
		t.Run(fixture.coin+"/"+fixture.exchange, func(t *testing.T) {
			payload := fmt.Sprintf(`{"coin":%q,"exchange":%q,"bid_price":"99.1234567800","bid_size":"1.2345678900","ask_price":"100.9876543200","ask_size":"2.3456789100","source_at":"2023-11-14T22:13:20.123Z","received_at":"2023-11-14T22:13:20.234567891Z","expires_at":"2023-11-14T22:13:22.234567891Z","invalidated":true,"future":true}`, fixture.coin, fixture.exchange)
			quote, allowed := store.decode(payload)
			if allowed != fixture.allowed {
				t.Fatalf("pair accepted = %v, want %v", allowed, fixture.allowed)
			}

			want := contracts.Quote{
				Coin:        fixture.coin,
				Exchange:    fixture.exchange,
				BidPrice:    "99.1234567800",
				BidSize:     "1.2345678900",
				AskPrice:    "100.9876543200",
				AskSize:     "2.3456789100",
				SourceAt:    time.Date(2023, 11, 14, 22, 13, 20, 123000000, time.UTC),
				ReceivedAt:  time.Date(2023, 11, 14, 22, 13, 20, 234567891, time.UTC),
				ExpiresAt:   time.Date(2023, 11, 14, 22, 13, 22, 234567891, time.UTC),
				Invalidated: true,
			}
			if allowed && !reflect.DeepEqual(quote, want) {
				t.Fatalf("decoded quote fields changed: %#v, want %#v", quote, want)
			}
		})
	}

	for _, payload := range []string{`{"coin":7,"exchange":"Bybit"}`, `{"coin":"ETH","exchange":[]}`, "{", "null"} {
		if _, accepted := store.decode(payload); accepted {
			t.Fatalf("invalid identity payload accepted: %s", payload)
		}
	}
}

func TestQuoteContractRejectsWrongMetadataTypesForAllowedPair(t *testing.T) {
	store := NewRedisQuoteStore(nil, []string{"XAUT"}, []string{"Bybit"})
	receivedAt := time.Now().UTC().Add(-time.Millisecond)
	want := contracts.Quote{
		Coin:       "XAUT",
		Exchange:   "Bybit",
		BidPrice:   "99.125",
		BidSize:    "1.75",
		AskPrice:   "100.25",
		AskSize:    "2.625",
		SourceAt:   receivedAt,
		ReceivedAt: receivedAt,
		ExpiresAt:  receivedAt.Add(5 * time.Second),
	}
	payload, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	if quote, accepted := store.decode(string(payload)); !accepted || !reflect.DeepEqual(quote, want) {
		t.Fatalf("valid baseline quote rejected or changed: %#v", quote)
	}

	for _, field := range []string{"invalidated", "source_at", "expires_at"} {
		t.Run(field, func(t *testing.T) {
			var fields map[string]json.RawMessage

			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatal(err)
			}

			fields[field] = json.RawMessage(`7`)
			invalidPayload, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}

			if quote, accepted := store.decode(string(invalidPayload)); accepted {
				t.Fatalf("allowed pair with wrongly typed %s accepted: %#v", field, quote)
			}
		})
	}
}

func TestSnapshotContractRejectsKeyPayloadMismatch(t *testing.T) {
	client := testsupport.RedisClient(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	receivedAt := time.Now().UTC().Add(-time.Second)

	want := contracts.Quote{
		Coin:       "BTC",
		Exchange:   "OKX",
		BidPrice:   "99.125",
		BidSize:    "1.75",
		AskPrice:   "100.25",
		AskSize:    "2.625",
		SourceAt:   receivedAt.Truncate(time.Millisecond),
		ReceivedAt: receivedAt,
		ExpiresAt:  receivedAt.Add(10 * time.Second),
	}

	for key, identity := range map[string][2]string{
		"fetcher:quotes:spot:ETH-USDT:Bybit": {"BTC", "Bybit"},
		"fetcher:quotes:spot:ETH-USDT:OKX":   {"ETH", "Bybit"},
		"fetcher:quotes:spot:BTC-USDT:Bybit": {"ETH", "OKX"},
		"fetcher:quotes:spot:BTC-USDT:OKX":   {"BTC", "OKX"},
	} {
		quote := want
		quote.Coin = identity[0]
		quote.Exchange = identity[1]
		payload, err := json.Marshal(quote)
		if err != nil {
			t.Fatal(err)
		}

		if err := client.Set(ctx, key, payload, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}

	store := NewRedisQuoteStore(client, []string{"ETH", "BTC"}, []string{"Bybit", "OKX"})
	quotes, err := store.snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(quotes, []contracts.Quote{want}) {
		t.Fatalf("snapshot admitted mismatched identities or lost quote fields: %#v", quotes)
	}
}
