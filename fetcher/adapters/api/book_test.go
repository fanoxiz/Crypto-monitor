package api

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

func TestQuoteFromBookPreservesIdentityAndFirstLevel(t *testing.T) {
	bids := [][]string{{"99.1234567800", "1.2345678900", "extra"}, {"98", "9"}}
	asks := [][]string{{"100.9876543200", "2.3456789100", "extra"}, {"101", "10"}}

	for _, identity := range [][2]string{{"ETH", "Bybit"}, {"XAUT", "OKX"}} {
		t.Run(identity[0]+"/"+identity[1], func(t *testing.T) {
			want := contracts.Quote{
				Coin:     identity[0],
				Exchange: identity[1],
				BidPrice: "99.1234567800",
				BidSize:  "1.2345678900",
				AskPrice: "100.9876543200",
				AskSize:  "2.3456789100",
			}
			quote := quoteFromBook(identity[0], identity[1], bids, asks)

			if !reflect.DeepEqual(quote, want) {
				t.Fatalf("normalized book = %#v, want %#v", quote, want)
			}
		})
	}
}

func TestQuoteFromBookIgnoresMissingOrMalformedSides(t *testing.T) {
	bids := [][]string{{"99", "1"}}
	asks := [][]string{{"100", "2"}}

	for name, sides := range map[string][2][][]string{
		"missing_bids":            {nil, asks},
		"missing_asks":            {bids, nil},
		"both_missing":            {nil, nil},
		"missing_bids_empty_asks": {nil, {}},
		"empty_bids_missing_asks": {{}, nil},
		"nil_bid_level":           {{nil}, asks},
		"nil_ask_level":           {bids, {nil}},
		"empty_bid_level":         {{{}}, asks},
		"empty_ask_level":         {bids, {{}}},
		"short_bid_level":         {{{"99"}}, asks},
		"short_ask_level":         {bids, {{"100"}}},
		"broken_first_bid_level":  {{{"99"}, bids[0]}, asks},
		"broken_first_ask_level":  {bids, {{"100"}, asks[0]}},
	} {
		t.Run(name, func(t *testing.T) {
			quote := quoteFromBook("XAUT", "Bitget", sides[0], sides[1])
			if !reflect.DeepEqual(quote, contracts.Quote{}) {
				t.Fatalf("incomplete book became a price or invalidation: %#v", quote)
			}
		})
	}

	t.Run("missing_coin", func(t *testing.T) {
		for _, sides := range [][2][][]string{{bids, asks}, {{}, {}}} {
			if quote := quoteFromBook("", "OKX", sides[0], sides[1]); !reflect.DeepEqual(quote, contracts.Quote{}) {
				t.Fatalf("book without coin became a price or invalidation: %#v", quote)
			}
		}
	})
}

func TestQuoteFromBookInvalidatesExplicitlyEmptySides(t *testing.T) {
	bids := [][]string{{"99", "1"}}
	asks := [][]string{{"100", "2"}}

	for _, identity := range [][2]string{{"ETH", "Bybit"}, {"XAUT", "OKX"}} {
		t.Run(identity[0]+"/"+identity[1], func(t *testing.T) {
			for name, sides := range map[string][2][][]string{
				"empty_bids": {{}, asks},
				"empty_asks": {bids, {}},
				"both_empty": {{}, {}},
			} {
				t.Run(name, func(t *testing.T) {
					want := contracts.Quote{
						Coin:        identity[0],
						Exchange:    identity[1],
						Invalidated: true,
					}
					quote := quoteFromBook(identity[0], identity[1], sides[0], sides[1])

					if !reflect.DeepEqual(quote, want) {
						t.Fatalf("empty book = %#v, want identity-preserving invalidation %#v", quote, want)
					}
				})
			}
		})
	}
}

func TestBookDecodersDoNotInvalidateACKWithEmptyBook(t *testing.T) {
	decoders := map[string]func([]byte) (streamMessage, error){
		"Bybit":  decodeBybit,
		"Bitget": decodeBitget,
		"OKX":    decodeOKX,
	}

	for _, fixture := range protocolFixtures() {
		decode, exists := decoders[fixture.exchange]
		if !exists {
			continue
		}

		t.Run(fixture.exchange, func(t *testing.T) {
			var fields map[string]json.RawMessage

			if err := json.Unmarshal([]byte(fixture.controls[0]), &fields); err != nil {
				t.Fatal(err)
			}

			fields["ts"] = json.RawMessage(`1700000000123`)
			fields["data"] = json.RawMessage(`[{"ts":"1700000000123","b":[],"a":[]}]`)
			if fixture.exchange == "Bybit" {
				fields["cts"] = json.RawMessage(`1700000000123`)
				fields["data"] = json.RawMessage(`{"s":"XAUTUSDT","b":[],"a":[]}`)
			} else if fixture.exchange == "OKX" {
				fields["data"] = json.RawMessage(`[{"ts":"1700000000123","bids":[],"asks":[]}]`)
			}

			payload, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}

			message, err := decode(payload)
			if err != nil || !reflect.DeepEqual(message.quote, contracts.Quote{}) {
				t.Fatalf("ACK data became a price or invalidation: %#v, %v", message, err)
			}
		})
	}
}
