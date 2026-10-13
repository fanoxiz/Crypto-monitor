package core

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

const contractQuoteJSON = `{"coin":"ETH","exchange":"Bybit","bid_price":"99.125","bid_size":"1.75","ask_price":"100.25","ask_size":"2.625","source_at":"2023-11-14T22:13:20.123Z","received_at":"2023-11-15T01:13:20.234567891+03:00","expires_at":"2023-11-14T22:13:22.234567891Z","future":{"value":true}}`

func TestQuoteContractPreservesMarketAndSignalFields(t *testing.T) {
	for _, coin := range []string{"ETH", "XAUT"} {
		t.Run(coin, func(t *testing.T) {
			var quote contracts.Quote
			payload := strings.Replace(contractQuoteJSON, `"coin":"ETH"`, `"coin":"`+coin+`"`, 1)

			if err := json.Unmarshal([]byte(payload), &quote); err != nil {
				t.Fatal(err)
			}

			receivedAt := time.Date(2023, 11, 14, 22, 13, 20, 234567891, time.UTC)
			expiresAt := receivedAt.Add(2 * time.Second)
			now := receivedAt.Add(time.Millisecond)
			analyzer := NewAnalyzerService(map[string]float64{
				"Bybit": 0.001,
				"OKX":   0.002,
			}, nil, nil, 3*time.Second)

			if quote.Coin != coin || quote.Exchange != "Bybit" || quote.Invalidated ||
				quote.BidPrice != "99.125" || quote.BidSize != "1.75" || quote.AskPrice != "100.25" || quote.AskSize != "2.625" ||
				!quote.SourceAt.Equal(time.Date(2023, 11, 14, 22, 13, 20, 123000000, time.UTC)) ||
				!quote.ReceivedAt.Equal(receivedAt) || !quote.ExpiresAt.Equal(expiresAt) {
				t.Fatalf("JSON quote fields changed: %#v", quote)
			}

			buy, valid := analyzer.parseQuote(quote, now)
			if !valid {
				t.Fatal("complete JSON quote with additional fields rejected")
			}

			if buy.exchange != "Bybit" || buy.bidPrice != 99.125 || buy.askPrice != 100.25 || buy.bidSize != 1.75 || buy.askSize != 2.625 ||
				!buy.receivedAt.Equal(receivedAt) || !buy.expiresAt.Equal(expiresAt) {
				t.Fatalf("internal market fields changed: %#v", buy)
			}

			quote.Exchange = "OKX"
			quote.BidPrice = "105.75"
			quote.AskPrice = "106.5"
			quote.BidSize = "1.5"
			quote.AskSize = "4.75"
			sell, valid := analyzer.parseQuote(quote, now)
			if !valid {
				t.Fatal("complete sale quote rejected")
			}

			deal, fingerprint, found := analyzer.findDeal(coin, []marketQuote{buy, sell}, now)
			if !found {
				t.Fatal("contract quotes produced no signal")
			}

			if deal.Coin != coin || deal.BuyExchange != "Bybit" || deal.SellExchange != "OKX" || !deal.DetectedAt.Equal(now) {
				t.Fatalf("signal identity or detection time changed: %#v", deal)
			}

			for name, amounts := range map[string][2]float64{
				"buy cost":      {buy.buyCost, 100.35025},
				"sale proceeds": {buy.sellProceeds, 99.025875},
				"buy amount":    {deal.BuyAmountUSDT, 150.525375},
				"sell amount":   {deal.SellAmountUSDT, 158.30775},
				"profit":        {deal.ProfitUSDT, 7.782375},
			} {
				if math.IsNaN(amounts[0]) || math.Abs(amounts[0]-amounts[1]) > 1e-9 {
					t.Errorf("%s = %.12f, want %.12f", name, amounts[0], amounts[1])
				}
			}

			wantFingerprint := dealFingerprint{
				buyExchange:  "Bybit",
				sellExchange: "OKX",
				buyPrice:     100.25,
				sellPrice:    105.75,
				quantity:     1.5,
			}
			if fingerprint != wantFingerprint {
				t.Fatalf("signal fingerprint = %#v, want %#v", fingerprint, wantFingerprint)
			}
		})
	}
}

func TestQuoteJSONContractRejectsIncompleteOrWronglyTypedPrices(t *testing.T) {
	var original map[string]json.RawMessage

	if err := json.Unmarshal([]byte(contractQuoteJSON), &original); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2023, 11, 14, 22, 13, 21, 0, time.UTC)
	analyzer := NewAnalyzerService(map[string]float64{
		"Bybit": 0.001,
	}, nil, nil, 3*time.Second)

	for _, field := range []string{"coin", "exchange", "bid_price", "bid_size", "ask_price", "ask_size", "received_at"} {
		for _, variant := range []string{"missing", "wrong_type"} {
			t.Run(field+"/"+variant, func(t *testing.T) {
				fields := make(map[string]json.RawMessage, len(original))

				for key, value := range original {
					fields[key] = value
				}

				if variant == "missing" {
					delete(fields, field)
				} else {
					fields[field] = json.RawMessage(`7`)
				}

				payload, err := json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}

				var quote contracts.Quote

				err = json.Unmarshal(payload, &quote)
				if variant == "wrong_type" {
					if err == nil {
						t.Fatal("wrongly typed contract field accepted")
					}

					return
				}

				if err != nil {
					t.Fatal(err)
				}

				if price, valid := analyzer.parseQuote(quote, now); valid {
					t.Fatalf("incomplete JSON became a usable price: %#v", price)
				}
			})
		}
	}

	for _, field := range []string{"source_at", "expires_at", "invalidated"} {
		t.Run(field+"/wrong_type", func(t *testing.T) {
			var fields map[string]json.RawMessage

			if err := json.Unmarshal([]byte(contractQuoteJSON), &fields); err != nil {
				t.Fatal(err)
			}

			fields[field] = json.RawMessage(`7`)
			payload, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}

			var quote contracts.Quote

			if err := json.Unmarshal(payload, &quote); err == nil {
				t.Fatalf("wrongly typed metadata field %s accepted", field)
			}
		})
	}
}

func TestInvalidationJSONContractCannotBecomePrice(t *testing.T) {
	var fields map[string]json.RawMessage

	if err := json.Unmarshal([]byte(contractQuoteJSON), &fields); err != nil {
		t.Fatal(err)
	}

	fields["invalidated"] = json.RawMessage(`true`)
	payload, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}

	var quote contracts.Quote

	if err := json.Unmarshal(payload, &quote); err != nil {
		t.Fatal(err)
	}

	if !quote.Invalidated || quote.Coin != "ETH" || quote.Exchange != "Bybit" {
		t.Fatalf("invalidation identity or flag lost: %#v", quote)
	}

	analyzer := NewAnalyzerService(map[string]float64{
		"Bybit": 0.001,
	}, nil, nil, 3*time.Second)
	now := time.Date(2023, 11, 14, 22, 13, 21, 0, time.UTC)

	if price, valid := analyzer.parseQuote(quote, now); valid {
		t.Fatalf("invalidation became a usable price: %#v", price)
	}
}
