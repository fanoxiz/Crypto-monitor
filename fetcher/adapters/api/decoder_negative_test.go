package api

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type decoderMessageFixture struct {
	exchange    string
	file        string
	decode      func([]byte) (streamMessage, error)
	identityKey string
	bidKey      string
	askKey      string
	want        contracts.Quote
}

func (f decoderMessageFixture) book(fields map[string]any) map[string]any {
	if f.exchange == "Binance" {
		return fields
	}

	if f.exchange == "Bybit" {
		return fields["data"].(map[string]any)
	}

	return fields["data"].([]any)[0].(map[string]any)
}

func (f decoderMessageFixture) identity(fields map[string]any) map[string]any {
	if f.exchange == "Binance" || f.exchange == "Bybit" {
		return f.book(fields)
	}

	return fields["arg"].(map[string]any)
}

func checkDecoderOutcome(t *testing.T, decode func([]byte) (streamMessage, error), payload []byte, want *contracts.Quote, wantDataError bool) {
	t.Helper()

	message, err := decode(payload)
	wantInvalidation := want != nil && want.Invalidated
	if message.quote.Invalidated != wantInvalidation {
		t.Fatalf("invalidation = %v, want %v; quote: %#v; error: %v", message.quote.Invalidated, wantInvalidation, message.quote, err)
	}

	if message.confirmedAll || message.confirmedCoin != "" || message.heartbeat {
		t.Fatalf("market payload became an ACK or heartbeat: %#v", message)
	}

	if wantDataError {
		if err == nil || errors.Is(err, errRequestRejected) {
			t.Fatalf("decoder error = %v, want payload error rather than an exchange request refusal", err)
		}

		return
	}

	if err != nil {
		t.Fatalf("unexpected decoder error: %v", err)
	}

	if want == nil {
		if message.quote.Coin != "" {
			t.Fatalf("ignored message emitted a quote: %#v", message.quote)
		}

		return
	}

	if !reflect.DeepEqual(message.quote, *want) {
		t.Fatalf("decoded quote = %#v, want %#v", message.quote, *want)
	}
}

func TestExchangeDecodersHandleMalformedAndIncompleteMessages(t *testing.T) {
	for _, fixture := range []decoderMessageFixture{
		{
			exchange:    "Binance",
			file:        "binance.json",
			decode:      decodeBinance,
			identityKey: "s",
			bidKey:      "b",
			askKey:      "a",
			want: contracts.Quote{
				Coin:     "BNB",
				Exchange: "Binance",
				BidPrice: "25.35190000",
				BidSize:  "31.21000000",
				AskPrice: "25.36520000",
				AskSize:  "40.66000000",
			},
		},
		{
			exchange:    "Bybit",
			file:        "bybit.json",
			decode:      decodeBybit,
			identityKey: "s",
			bidKey:      "b",
			askKey:      "a",
			want: contracts.Quote{
				Coin:     "BTC",
				Exchange: "Bybit",
				BidPrice: "82350.8",
				BidSize:  "0.359133",
				AskPrice: "82350.9",
				AskSize:  "1.618575",
				SourceAt: time.Date(2026, 10, 15, 21, 52, 34, 264000000, time.UTC),
			},
		},
		{
			exchange:    "Bitget",
			file:        "bitget.json",
			decode:      decodeBitget,
			identityKey: "symbol",
			bidKey:      "b",
			askKey:      "a",
			want: contracts.Quote{
				Coin:     "BTC",
				Exchange: "Bitget",
				BidPrice: "82352.32",
				BidSize:  "0.756931",
				AskPrice: "82352.33",
				AskSize:  "0.465895",
				SourceAt: time.Date(2026, 10, 15, 21, 52, 35, 88000000, time.UTC),
			},
		},
		{
			exchange:    "OKX",
			file:        "okx.json",
			decode:      decodeOKX,
			identityKey: "instId",
			bidKey:      "bids",
			askKey:      "asks",
			want: contracts.Quote{
				Coin:     "BTC",
				Exchange: "OKX",
				BidPrice: "82348",
				BidSize:  "0.31029845",
				AskPrice: "82348.1",
				AskSize:  "0.58071666",
				SourceAt: time.Date(2026, 10, 15, 21, 52, 34, 809000000, time.UTC),
			},
		},
	} {
		t.Run(fixture.exchange, func(t *testing.T) {
			original, err := os.ReadFile(filepath.Join("testdata", "decoder", fixture.file))
			if err != nil {
				t.Fatal(err)
			}

			t.Run("valid_control", func(t *testing.T) {
				checkDecoderOutcome(t, fixture.decode, original, &fixture.want, false)
			})

			for name, payload := range map[string]string{
				"empty_payload": "",
				"broken_json":   "{",
				"array_root":    "[]",
				"number_root":   "7",
			} {
				t.Run(name, func(t *testing.T) {
					checkDecoderOutcome(t, fixture.decode, []byte(payload), nil, true)
				})
			}

			for name, payload := range map[string]string{
				"null_root":    "null",
				"empty_object": "{}",
			} {
				t.Run(name, func(t *testing.T) {
					checkDecoderOutcome(t, fixture.decode, []byte(payload), nil, false)
				})
			}

			for _, variant := range []string{"missing", "null", "number"} {
				t.Run("identity/"+variant, func(t *testing.T) {
					payload := mutateDecoderMessage(t, original, func(fields map[string]any) {
						identity := fixture.identity(fields)
						switch variant {
						case "missing":
							delete(identity, fixture.identityKey)
						case "null":
							identity[fixture.identityKey] = nil
						case "number":
							identity[fixture.identityKey] = json.Number("7")
						}
					})

					checkDecoderOutcome(t, fixture.decode, payload, nil, variant == "number")
				})
			}

			if fixture.exchange == "Binance" {
				for _, field := range []string{"b", "B", "a", "A"} {
					for _, variant := range []string{"missing", "null", "number"} {
						t.Run(field+"/"+variant, func(t *testing.T) {
							payload := mutateDecoderMessage(t, original, func(fields map[string]any) {
								switch variant {
								case "missing":
									delete(fields, field)
								case "null":
									fields[field] = nil
								case "number":
									fields[field] = json.Number("7")
								}
							})

							if variant == "number" {
								checkDecoderOutcome(t, fixture.decode, payload, nil, true)

								return
							}

							want := fixture.want
							switch field {
							case "b":
								want.BidPrice = ""
							case "B":
								want.BidSize = ""
							case "a":
								want.AskPrice = ""
							case "A":
								want.AskSize = ""
							}

							checkDecoderOutcome(t, fixture.decode, payload, &want, false)
						})
					}
				}

				return
			}

			for _, variant := range []string{"missing", "null", "empty", "wrong_type", "null_item"} {
				if variant == "null_item" && fixture.exchange == "Bybit" {
					continue
				}

				t.Run("data/"+variant, func(t *testing.T) {
					payload := mutateDecoderMessage(t, original, func(fields map[string]any) {
						switch variant {
						case "missing":
							delete(fields, "data")
						case "null":
							fields["data"] = nil
						case "empty":
							if fixture.exchange == "Bybit" {
								fields["data"] = map[string]any{}
							} else {
								fields["data"] = []any{}
							}
						case "wrong_type":
							fields["data"] = "bad-data"
						case "null_item":
							fields["data"] = []any{nil}
						}
					})

					checkDecoderOutcome(t, fixture.decode, payload, nil, variant == "wrong_type")
				})
			}

			invalidation := contracts.Quote{
				Coin:        fixture.want.Coin,
				Exchange:    fixture.exchange,
				Invalidated: true,
				SourceAt:    fixture.want.SourceAt,
			}

			for _, side := range []string{fixture.bidKey, fixture.askKey} {
				for _, variant := range []string{"missing", "null", "null_level", "empty_level", "short_level", "numeric_price", "non_array", "empty"} {
					t.Run(side+"/"+variant, func(t *testing.T) {
						payload := mutateDecoderMessage(t, original, func(fields map[string]any) {
							book := fixture.book(fields)
							switch variant {
							case "missing":
								delete(book, side)
							case "null":
								book[side] = nil
							case "null_level":
								book[side] = []any{nil}
							case "empty_level":
								book[side] = []any{[]any{}}
							case "short_level":
								book[side] = []any{[]any{"99"}}
							case "numeric_price":
								book[side].([]any)[0].([]any)[0] = json.Number("7")
							case "non_array":
								book[side] = "bad-side"
							case "empty":
								book[side] = []any{}
							}
						})

						var want *contracts.Quote
						if variant == "empty" {
							want = &invalidation
						}

						checkDecoderOutcome(t, fixture.decode, payload, want, variant == "numeric_price" || variant == "non_array")
					})
				}

				other := fixture.bidKey
				if side == fixture.bidKey {
					other = fixture.askKey
				}

				t.Run(side+"/empty_other_missing", func(t *testing.T) {
					payload := mutateDecoderMessage(t, original, func(fields map[string]any) {
						book := fixture.book(fields)
						book[side] = []any{}
						delete(book, other)
					})

					checkDecoderOutcome(t, fixture.decode, payload, nil, false)
				})
			}

			t.Run("both_empty", func(t *testing.T) {
				payload := mutateDecoderMessage(t, original, func(fields map[string]any) {
					book := fixture.book(fields)
					book[fixture.bidKey] = []any{}
					book[fixture.askKey] = []any{}
				})

				checkDecoderOutcome(t, fixture.decode, payload, &invalidation, false)
			})
		})
	}
}
