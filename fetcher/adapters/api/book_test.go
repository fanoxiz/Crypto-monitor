package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

func TestDecodeBookQuotes(t *testing.T) {
	exchanges := []struct {
		name      string
		decode    func([]byte) (contracts.Quote, error)
		book      string
		emptyData string
		control   string
	}{
		{
			name:      "Bybit",
			decode:    decodeBybit,
			book:      `{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000200,"cts":1700000000000,"data":{"s":"BTCUSDT","b":%s,"a":%s}}`,
			emptyData: `{"data":{}}`,
			control:   `{"success":true,"op":"subscribe"}`,
		},
		{
			name:      "Bitget",
			decode:    decodeBitget,
			book:      `{"arg":{"instType":"spot","topic":"books1","symbol":"BTCUSDT"},"action":"snapshot","ts":1700000000200,"data":[{"b":%s,"a":%s,"ts":"1700000000000"}]}`,
			emptyData: `{"data":[]}`,
			control:   `{"event":"subscribe","arg":{"instType":"spot","topic":"books1","symbol":"BTCUSDT"}}`,
		},
		{
			name:      "OKX",
			decode:    decodeOKX,
			book:      `{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":%s,"asks":%s,"ts":"1700000000000"}]}`,
			emptyData: `{"data":[]}`,
			control:   `{"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT"}}`,
		},
	}

	for _, exchange := range exchanges {
		t.Run(exchange.name, func(t *testing.T) {
			bids := `[["100.1","3.2","ignored"],["90","20"]]`
			asks := `[["101.3","4.4","ignored"],["110","30"]]`
			want := contracts.Quote{
				Coin:     "BTC",
				Exchange: exchange.name,
				BidPrice: "100.1",
				BidSize:  "3.2",
				AskPrice: "101.3",
				AskSize:  "4.4",
				SourceAt: time.UnixMilli(1700000000000).UTC(),
			}
			books := []struct {
				name  string
				bids  string
				asks  string
				valid bool
			}{
				{
					name:  "first_level",
					bids:  bids,
					asks:  asks,
					valid: true,
				},
				{
					name: "empty_bids",
					bids: `[]`,
					asks: asks,
				},
				{
					name: "empty_asks",
					bids: bids,
					asks: `[]`,
				},
				{
					name: "short_bid",
					bids: `[["100.1"]]`,
					asks: asks,
				},
				{
					name: "short_ask",
					bids: bids,
					asks: `[["101.3"]]`,
				},
				{
					name: "null_bid_level",
					bids: `[null]`,
					asks: asks,
				},
				{
					name: "null_ask_level",
					bids: bids,
					asks: `[null]`,
				},
			}

			for _, book := range books {
				t.Run(book.name, func(t *testing.T) {
					raw := fmt.Sprintf(exchange.book, book.bids, book.asks)
					quote, err := exchange.decode([]byte(raw))
					if err != nil {
						t.Fatalf("decode failed: %v", err)
					}

					expected := contracts.Quote{}
					if book.valid {
						expected = want
					} else if book.name == "empty_bids" || book.name == "empty_asks" {
						expected = contracts.Quote{
							Coin:        "BTC",
							Exchange:    exchange.name,
							Invalidated: true,
							SourceAt:    time.UnixMilli(1700000000000).UTC(),
						}
					}

					if quote != expected {
						t.Fatalf("expected %+v, got %+v", expected, quote)
					}
				})
			}

			messages := []struct {
				name    string
				raw     string
				wantErr bool
			}{
				{
					name: "missing_data",
					raw:  `{}`,
				},
				{
					name: "empty_data",
					raw:  exchange.emptyData,
				},
				{
					name: "control",
					raw:  exchange.control,
				},
				{
					name:    "malformed_json",
					raw:     `{"data":`,
					wantErr: true,
				},
			}

			for _, message := range messages {
				t.Run(message.name, func(t *testing.T) {
					quote, err := exchange.decode([]byte(message.raw))
					if (err != nil) != message.wantErr {
						t.Fatalf("expected error=%t, got %v", message.wantErr, err)
					}

					if quote != (contracts.Quote{}) {
						t.Fatalf("expected empty quote, got %+v", quote)
					}
				})
			}
		})
	}
}
