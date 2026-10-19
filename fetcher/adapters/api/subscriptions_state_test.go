package api

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

func TestInstrumentStatusTracksIndependentLifecycle(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	start := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	dataAt := start.Add(time.Second)
	quoteAt := start.Add(2 * time.Second)
	invalidatedAt := start.Add(3 * time.Second)
	partialAt := start.Add(4 * time.Second)
	recoveredAt := start.Add(5 * time.Second)
	instruments := map[string]*instrumentStatus{
		"ETH":  {},
		"XAUT": {},
	}

	for _, step := range []struct {
		name    string
		message streamMessage
		at      time.Time
		want    instrumentStatus
	}{
		{
			name: "ack_without_data",
			message: streamMessage{
				confirmedCoin: "ETH",
			},
			at: start,
			want: instrumentStatus{
				confirmed: true,
			},
		},
		{
			name: "market_data_without_quote",
			message: streamMessage{
				dataCoin: "ETH",
			},
			at: dataAt,
			want: instrumentStatus{
				confirmed:  true,
				lastDataAt: dataAt,
			},
		},
		{
			name: "quote",
			message: streamMessage{
				dataCoin: "ETH",
				quote: contracts.Quote{
					Coin: "ETH",
				},
			},
			at: quoteAt,
			want: instrumentStatus{
				confirmed:   true,
				lastDataAt:  quoteAt,
				lastQuoteAt: quoteAt,
			},
		},
		{
			name: "invalidation",
			message: streamMessage{
				dataCoin: "ETH",
				quote: contracts.Quote{
					Coin:        "ETH",
					Invalidated: true,
				},
			},
			at: invalidatedAt,
			want: instrumentStatus{
				confirmed:   true,
				lastDataAt:  invalidatedAt,
				lastQuoteAt: invalidatedAt,
				invalidated: true,
			},
		},
		{
			name: "partial_data_does_not_restore_quote",
			message: streamMessage{
				dataCoin: "ETH",
			},
			at: partialAt,
			want: instrumentStatus{
				confirmed:   true,
				lastDataAt:  partialAt,
				lastQuoteAt: invalidatedAt,
				invalidated: true,
			},
		},
		{
			name: "quote_restores_instrument",
			message: streamMessage{
				dataCoin: "ETH",
				quote: contracts.Quote{
					Coin: "ETH",
				},
			},
			at: recoveredAt,
			want: instrumentStatus{
				confirmed:   true,
				lastDataAt:  recoveredAt,
				lastQuoteAt: recoveredAt,
			},
		},
		{
			name: "duplicate_ack_preserves_market_state",
			message: streamMessage{
				confirmedCoin: "ETH",
			},
			at: recoveredAt.Add(time.Second),
			want: instrumentStatus{
				confirmed:   true,
				lastDataAt:  recoveredAt,
				lastQuoteAt: recoveredAt,
			},
		},
	} {
		t.Run(step.name, func(t *testing.T) {
			recordMessage(logger, instruments, step.message, step.at)

			if len(instruments) != 2 || !reflect.DeepEqual(*instruments["ETH"], step.want) || *instruments["XAUT"] != (instrumentStatus{}) {
				t.Fatalf("instrument lifecycle: ETH=%#v, XAUT=%#v, want ETH=%#v and untouched XAUT", instruments["ETH"], instruments["XAUT"], step.want)
			}
		})
	}
}

func TestInstrumentStatusKeepsTwoActiveInstrumentsIndependent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	start := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	instruments := map[string]*instrumentStatus{
		"ETH": {
			confirmed: true,
		},
		"XAUT": {
			confirmed: true,
		},
	}
	apply := func(coin string, at time.Time, invalidated bool, wantETH, wantXAUT instrumentStatus) {
		t.Helper()

		recordMessage(logger, instruments, streamMessage{
			dataCoin: coin,
			quote: contracts.Quote{
				Coin:        coin,
				Invalidated: invalidated,
			},
		}, at)

		if len(instruments) != 2 || *instruments["ETH"] != wantETH || *instruments["XAUT"] != wantXAUT {
			t.Fatalf("after %s at %s: ETH=%#v, XAUT=%#v; want ETH=%#v, XAUT=%#v", coin, at, *instruments["ETH"], *instruments["XAUT"], wantETH, wantXAUT)
		}
	}

	ethQuote := instrumentStatus{
		confirmed:   true,
		lastDataAt:  start.Add(time.Second),
		lastQuoteAt: start.Add(time.Second),
	}
	apply("ETH", start.Add(time.Second), false, ethQuote, instrumentStatus{
		confirmed: true,
	})

	xautQuote := instrumentStatus{
		confirmed:   true,
		lastDataAt:  start.Add(2 * time.Second),
		lastQuoteAt: start.Add(2 * time.Second),
	}
	apply("XAUT", start.Add(2*time.Second), false, ethQuote, xautQuote)

	ethInvalidated := instrumentStatus{
		confirmed:   true,
		lastDataAt:  start.Add(3 * time.Second),
		lastQuoteAt: start.Add(3 * time.Second),
		invalidated: true,
	}
	apply("ETH", start.Add(3*time.Second), true, ethInvalidated, xautQuote)

	xautUpdated := instrumentStatus{
		confirmed:   true,
		lastDataAt:  start.Add(4 * time.Second),
		lastQuoteAt: start.Add(4 * time.Second),
	}
	apply("XAUT", start.Add(4*time.Second), false, ethInvalidated, xautUpdated)

	ethRecovered := instrumentStatus{
		confirmed:   true,
		lastDataAt:  start.Add(5 * time.Second),
		lastQuoteAt: start.Add(5 * time.Second),
	}
	apply("ETH", start.Add(5*time.Second), false, ethRecovered, xautUpdated)

	xautInvalidated := instrumentStatus{
		confirmed:   true,
		lastDataAt:  start.Add(6 * time.Second),
		lastQuoteAt: start.Add(6 * time.Second),
		invalidated: true,
	}
	apply("XAUT", start.Add(6*time.Second), true, ethRecovered, xautInvalidated)

	ethUpdated := instrumentStatus{
		confirmed:   true,
		lastDataAt:  start.Add(7 * time.Second),
		lastQuoteAt: start.Add(7 * time.Second),
	}
	apply("ETH", start.Add(7*time.Second), false, ethUpdated, xautInvalidated)

	xautRecovered := instrumentStatus{
		confirmed:   true,
		lastDataAt:  start.Add(8 * time.Second),
		lastQuoteAt: start.Add(8 * time.Second),
	}
	apply("XAUT", start.Add(8*time.Second), false, ethUpdated, xautRecovered)
}

func TestInstrumentStatusHandlesGlobalAndUnexpectedAcknowledgments(t *testing.T) {
	for _, row := range []struct {
		name          string
		message       streamMessage
		confirmedXAUT bool
		wantEvent     string
		wantCoin      string
	}{
		{
			name: "global_ack",
			message: streamMessage{
				confirmedAll: true,
			},
			confirmedXAUT: true,
			wantEvent:     "subscription_confirmed",
			wantCoin:      "XAUT",
		},
		{
			name: "duplicate_ack",
			message: streamMessage{
				confirmedCoin: "ETH",
			},
		},
		{
			name: "unexpected_ack",
			message: streamMessage{
				confirmedCoin: "BTC",
			},
			wantEvent: "subscription_unexpected",
			wantCoin:  "BTC",
		},
		{
			name: "unexpected_market_data",
			message: streamMessage{
				dataCoin: "BTC",
				quote: contracts.Quote{
					Coin: "BTC",
				},
			},
			wantEvent: "market_data_unexpected",
			wantCoin:  "BTC",
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			var logs reconnectLogBuffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			at := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
			original := instrumentStatus{
				confirmed:   true,
				lastDataAt:  at.Add(-time.Second),
				lastQuoteAt: at.Add(-2 * time.Second),
				invalidated: true,
			}
			eth := original
			instruments := map[string]*instrumentStatus{
				"ETH":  &eth,
				"XAUT": {},
			}

			recordMessage(logger, instruments, row.message, at)

			if len(instruments) != 2 || *instruments["ETH"] != original || *instruments["XAUT"] != (instrumentStatus{
				confirmed: row.confirmedXAUT,
			}) {
				t.Fatalf("ACK or unexpected instrument changed unrelated state: %#v", instruments)
			}

			records := subscriptionLogRecords(t, &logs)
			if row.wantEvent == "" {
				if len(records) != 0 {
					t.Fatalf("duplicate ACK repeated confirmation: %#v", records)
				}

				return
			}

			if len(records) != 1 || records[0].Message != row.wantEvent || records[0].Coin != row.wantCoin {
				t.Fatalf("missing or ambiguous instrument diagnostic: %#v", records)
			}
		})
	}
}

func TestBookDecodersDistinguishObservedDataFromQuote(t *testing.T) {
	for _, fixture := range []decoderMessageFixture{
		{
			exchange: "Bybit",
			file:     "bybit.json",
			decode:   decodeBybit,
			bidKey:   "b",
		},
		{
			exchange: "Bitget",
			file:     "bitget.json",
			decode:   decodeBitget,
			bidKey:   "b",
		},
		{
			exchange: "OKX",
			file:     "okx.json",
			decode:   decodeOKX,
			bidKey:   "bids",
		},
	} {
		t.Run(fixture.exchange, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join("testdata", "decoder", fixture.file))
			if err != nil {
				t.Fatal(err)
			}

			partial := mutateDecoderMessage(t, source, func(fields map[string]any) {
				delete(fixture.book(fields), fixture.bidKey)
			})
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			instruments := map[string]*instrumentStatus{
				"BTC":  {},
				"XAUT": {},
			}
			at := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
			message, err := fixture.decode(partial)
			if err != nil || message.dataCoin != "BTC" || !reflect.DeepEqual(message.quote, contracts.Quote{}) {
				t.Fatalf("incomplete book lost its instrument or became a quote: %#v, error=%v", message, err)
			}

			recordMessage(logger, instruments, message, at)
			if *instruments["BTC"] != (instrumentStatus{
				lastDataAt: at,
			}) || *instruments["XAUT"] != (instrumentStatus{}) {
				t.Fatalf("observed data was confused with ACK or a quote: %#v", instruments["BTC"])
			}

			message, err = fixture.decode(source)
			if err != nil || message.dataCoin != "BTC" || message.quote.Coin != "BTC" || message.quote.Invalidated {
				t.Fatalf("complete book control was rejected: %#v, error=%v", message, err)
			}

			at = at.Add(time.Second)
			recordMessage(logger, instruments, message, at)
			if *instruments["BTC"] != (instrumentStatus{
				lastDataAt:  at,
				lastQuoteAt: at,
			}) || *instruments["XAUT"] != (instrumentStatus{}) {
				t.Fatalf("parsed quote did not update only its instrument: %#v", instruments["BTC"])
			}
		})
	}
}

func TestInstrumentStatusReportsCoverageAndAges(t *testing.T) {
	for _, row := range []struct {
		name        string
		confirmed   bool
		hasData     bool
		hasQuote    bool
		invalidated bool
		dataAge     time.Duration
		quoteAge    time.Duration
		level       string
	}{
		{
			name:  "awaiting_ack_and_data",
			level: "WARN",
		},
		{
			name:      "ack_without_market_data",
			confirmed: true,
			level:     "WARN",
		},
		{
			name:      "data_without_quote",
			confirmed: true,
			hasData:   true,
			dataAge:   2 * time.Second,
			level:     "WARN",
		},
		{
			name:     "quote_without_ack",
			hasData:  true,
			hasQuote: true,
			dataAge:  time.Second,
			quoteAge: 3 * time.Second,
			level:    "WARN",
		},
		{
			name:      "confirmed_with_quote",
			confirmed: true,
			hasData:   true,
			hasQuote:  true,
			dataAge:   time.Second,
			quoteAge:  3 * time.Second,
			level:     "INFO",
		},
		{
			name:        "invalidated",
			confirmed:   true,
			hasData:     true,
			hasQuote:    true,
			invalidated: true,
			dataAge:     2 * time.Second,
			quoteAge:    2 * time.Second,
			level:       "WARN",
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var logs reconnectLogBuffer
				logger := slog.New(slog.NewJSONHandler(&logs, nil))
				now := time.Now().UTC()
				state := instrumentStatus{
					confirmed:   row.confirmed,
					invalidated: row.invalidated,
				}
				if row.hasData {
					state.lastDataAt = now.Add(-row.dataAge)
				}
				if row.hasQuote {
					state.lastQuoteAt = now.Add(-row.quoteAge)
				}
				original := state
				instruments := map[string]*instrumentStatus{
					"XAUT": &state,
				}

				logInstrumentStatus(context.Background(), logger, instruments, []string{"XAUT"}, "coverage_check")

				records := subscriptionLogRecords(t, &logs)
				if len(records) != 1 {
					t.Fatalf("instrument reports = %d, want one", len(records))
				}
				got := records[0]
				if got.Message != "instrument_status" || got.Level != row.level || got.Coin != "XAUT" || got.Reason != "coverage_check" ||
					got.Confirmed != row.confirmed || got.DataReceived != row.hasData || got.QuoteReceived != row.hasQuote ||
					got.Invalidated != row.invalidated || !got.LastDataAt.Equal(original.lastDataAt) || !got.LastQuoteAt.Equal(original.lastQuoteAt) {
					t.Fatalf("incorrect instrument coverage report: %#v", got)
				}

				if (got.DataAge != nil) != row.hasData || (got.QuoteAge != nil) != row.hasQuote {
					t.Fatalf("ages do not match observed data and quotes: %#v", got)
				}
				if got.DataAge != nil && *got.DataAge != int64(row.dataAge) {
					t.Fatalf("data age = %d, want %d nanoseconds", *got.DataAge, row.dataAge)
				}
				if got.QuoteAge != nil && *got.QuoteAge != int64(row.quoteAge) {
					t.Fatalf("quote age = %d, want %d nanoseconds", *got.QuoteAge, row.quoteAge)
				}
				if state != original {
					t.Fatal("reporting changed the instrument state")
				}
			})
		})
	}
}
