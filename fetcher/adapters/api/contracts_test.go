package api

import (
	"reflect"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

func TestExchangeMessagesPreserveQuoteContract(t *testing.T) {
	for _, fixture := range []struct {
		exchange   string
		decode     func([]byte) (streamMessage, error)
		payload    string
		incomplete string
		wrongType  string
		emptyBook  string
	}{
		{
			exchange:   "Binance",
			decode:     decodeBinance,
			payload:    `{"s":"ETHUSDT","b":"99.1234567800","B":"1.2345678900","a":"100.9876543200","A":"2.3456789100","E":1700000000123,"future":{"value":true}}`,
			incomplete: `{"s":"ETHUSDT","b":"99","B":"1","a":"100"}`,
			wrongType:  `{"s":"ETHUSDT","b":99,"B":"1","a":"100","A":"2"}`,
		},
		{
			exchange:   "Bybit",
			decode:     decodeBybit,
			payload:    `{"cts":1700000000123,"data":{"s":"ETHUSDT","b":[["99.1234567800","1.2345678900"]],"a":[["100.9876543200","2.3456789100"]]},"future":{"value":true}}`,
			incomplete: `{"data":{"s":"ETHUSDT","b":[["99","1"]]}}`,
			wrongType:  `{"data":{"s":"ETHUSDT","b":[[99,"1"]],"a":[["100","2"]]}}`,
			emptyBook:  `{"data":{"s":"ETHUSDT","b":[],"a":[["100","2"]]}}`,
		},
		{
			exchange:   "Bitget",
			decode:     decodeBitget,
			payload:    `{"arg":{"symbol":"ETHUSDT"},"data":[{"b":[["99.1234567800","1.2345678900"]],"a":[["100.9876543200","2.3456789100"]],"ts":"1700000000123"}],"future":{"value":true}}`,
			incomplete: `{"arg":{"symbol":"ETHUSDT"},"data":[{"b":[["99","1"]]}]}`,
			wrongType:  `{"arg":{"symbol":"ETHUSDT"},"data":[{"b":[[99,"1"]],"a":[["100","2"]]}]}`,
			emptyBook:  `{"arg":{"symbol":"ETHUSDT"},"data":[{"b":[],"a":[["100","2"]]}]}`,
		},
		{
			exchange:   "OKX",
			decode:     decodeOKX,
			payload:    `{"arg":{"instId":"ETH-USDT"},"data":[{"bids":[["99.1234567800","1.2345678900"]],"asks":[["100.9876543200","2.3456789100"]],"ts":"1700000000123"}],"future":{"value":true}}`,
			incomplete: `{"arg":{"instId":"ETH-USDT"},"data":[{"bids":[["99","1"]]}]}`,
			wrongType:  `{"arg":{"instId":"ETH-USDT"},"data":[{"bids":[[99,"1"]],"asks":[["100","2"]]}]}`,
			emptyBook:  `{"arg":{"instId":"ETH-USDT"},"data":[{"bids":[],"asks":[["100","2"]]}]}`,
		},
	} {
		t.Run(fixture.exchange, func(t *testing.T) {
			message, err := fixture.decode([]byte(fixture.payload))
			if err != nil {
				t.Fatal(err)
			}

			want := contracts.Quote{
				Coin:     "ETH",
				Exchange: fixture.exchange,
				BidPrice: "99.1234567800",
				BidSize:  "1.2345678900",
				AskPrice: "100.9876543200",
				AskSize:  "2.3456789100",
			}
			if fixture.exchange != "Binance" {
				want.SourceAt = time.Date(2023, 11, 14, 22, 13, 20, 123000000, time.UTC)
			}

			if !reflect.DeepEqual(message.quote, want) || message.dataCoin != "ETH" {
				t.Fatalf("decoded message = %#v, want quote %#v and ETH identity", message, want)
			}

			if fixture.emptyBook != "" {
				t.Run("invalidation", func(t *testing.T) {
					message, err := fixture.decode([]byte(fixture.emptyBook))
					if err != nil {
						t.Fatal(err)
					}

					want := contracts.Quote{
						Coin:        "ETH",
						Exchange:    fixture.exchange,
						Invalidated: true,
					}
					if !reflect.DeepEqual(message.quote, want) || message.dataCoin != "ETH" {
						t.Fatalf("empty book lost invalidation identity or flag: %#v", message)
					}
				})
			}

			for name, payload := range map[string]string{
				"missing_fields": fixture.incomplete,
				"wrong_type":     fixture.wrongType,
				"broken_json":    "{",
				"null":           "null",
			} {
				t.Run(name, func(t *testing.T) {
					message, err := fixture.decode([]byte(payload))
					if (name == "wrong_type" || name == "broken_json") && err == nil {
						t.Fatal("invalid message type or JSON was accepted")
					}

					quote := message.quote
					if quote.Coin != "" && quote.Exchange != "" && quote.BidPrice != "" && quote.BidSize != "" && quote.AskPrice != "" && quote.AskSize != "" {
						t.Fatalf("incomplete message became a complete quote: %#v", quote)
					}
				})
			}
		})
	}
}
