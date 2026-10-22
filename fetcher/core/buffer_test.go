package core

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type bufferTestPair struct {
	coin     string
	exchange string
}

func bufferTestQuote(coin, exchange, price string, at time.Time, invalidated bool) contracts.Quote {
	quote := contracts.Quote{
		Coin:        coin,
		Exchange:    exchange,
		BidPrice:    price,
		BidSize:     "2.25",
		AskPrice:    "200.125",
		AskSize:     "3.75",
		Invalidated: invalidated,
		SourceAt:    at.Add(-10 * time.Millisecond),
		ReceivedAt:  at,
		ExpiresAt:   at.Add(600 * time.Millisecond),
	}
	if invalidated {
		quote.BidPrice = ""
		quote.BidSize = ""
		quote.AskPrice = ""
		quote.AskSize = ""
	}

	return quote
}

func assertQuoteBatch(t *testing.T, actual []contracts.Quote, expected ...contracts.Quote) {
	t.Helper()

	if len(actual) != len(expected) {
		t.Fatalf("batch = %#v, want %#v", actual, expected)
	}
	indexed := make(map[bufferTestPair]contracts.Quote, len(actual))
	for _, quote := range actual {
		pair := bufferTestPair{
			coin:     quote.Coin,
			exchange: quote.Exchange,
		}
		if _, exists := indexed[pair]; exists {
			t.Fatalf("duplicate pair in batch: %#v", pair)
		}

		indexed[pair] = quote
	}

	for _, quote := range expected {
		pair := bufferTestPair{
			coin:     quote.Coin,
			exchange: quote.Exchange,
		}
		if got, exists := indexed[pair]; !exists || !reflect.DeepEqual(got, quote) {
			t.Fatalf("pair %#v = %#v, want %#v", pair, got, quote)
		}
	}
}

func takeBufferNotification(t *testing.T, buffer *quoteBuffer) {
	t.Helper()

	select {
	case <-buffer.updates:
	default:
		t.Fatal("pending quotes have no wake-up notification")
	}
}

func TestQuoteBufferSeparatesPairsAndKeepsLatestArrival(t *testing.T) {
	buffer := newQuoteBuffer()
	at := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	ethBinance := bufferTestQuote("ETH", "Binance", "99.1", at, false)
	ethBybit := bufferTestQuote("ETH", "Bybit", "99.2", at, false)
	xautBinance := bufferTestQuote("XAUT", "Binance", "99.3", at, false)
	xautBybit := bufferTestQuote("XAUT", "Bybit", "99.4", at, false)
	for _, quote := range []contracts.Quote{ethBinance, ethBybit, xautBinance, xautBybit} {
		buffer.put(quote)
	}

	for _, price := range []string{"100.1", "100.2", "100.3"} {
		ethBinance = bufferTestQuote("ETH", "Binance", price, at.Add(time.Second), false)
		buffer.put(ethBinance)
	}

	takeBufferNotification(t, buffer)
	assertQuoteBatch(t, buffer.take(), ethBinance, ethBybit, xautBinance, xautBybit)
	assertQuoteBatch(t, buffer.take())
}

func TestQuoteBufferReplacesQuotesAndInvalidations(t *testing.T) {
	for _, before := range []bool{false, true} {
		for _, after := range []bool{false, true} {
			for _, offset := range []time.Duration{-time.Second, 0, time.Second} {
				name := fmt.Sprintf("invalidated_%v_to_%v/received_offset_%s", before, after, offset)
				t.Run(name, func(t *testing.T) {
					buffer := newQuoteBuffer()
					at := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
					old := bufferTestQuote("ETH", "Bybit", "99", at, before)
					newAt := at.Add(offset)
					latest := bufferTestQuote("ETH", "Bybit", "101", newAt, after)
					latest.SourceAt = at.Add(-time.Second)
					latest.ExpiresAt = at.Add(2 * time.Second)
					neighbor := bufferTestQuote("XAUT", "Bybit", "88", at, false)

					buffer.put(old)
					buffer.put(neighbor)
					buffer.put(latest)

					assertQuoteBatch(t, buffer.take(), latest, neighbor)
				})
			}
		}
	}
}

func TestQuoteBufferTakePreservesSubsequentUpdates(t *testing.T) {
	buffer := newQuoteBuffer()
	at := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	old := bufferTestQuote("ETH", "Binance", "99", at, false)
	neighbor := bufferTestQuote("XAUT", "Binance", "98", at, false)
	buffer.put(old)
	buffer.put(neighbor)
	takeBufferNotification(t, buffer)
	batch := buffer.take()

	latest := bufferTestQuote("ETH", "Binance", "101", at.Add(time.Second), false)
	other := bufferTestQuote("ETH", "Bybit", "102", at.Add(time.Second), true)
	buffer.put(latest)
	buffer.put(other)

	assertQuoteBatch(t, batch, old, neighbor)
	takeBufferNotification(t, buffer)
	assertQuoteBatch(t, buffer.take(), latest, other)
	assertQuoteBatch(t, buffer.take())
}

func TestQuoteBufferCoalescesAndRearmsNotifications(t *testing.T) {
	buffer := newQuoteBuffer()
	at := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	var latest contracts.Quote
	for index := range 100 {
		latest = bufferTestQuote("ETH", "Binance", fmt.Sprint(index), at.Add(time.Duration(index)*time.Millisecond), index%2 == 1)
		buffer.put(latest)
	}

	takeBufferNotification(t, buffer)
	select {
	case <-buffer.updates:
		t.Fatal("each update accumulated a separate wake-up notification")
	default:
	}
	batch := buffer.take()
	assertQuoteBatch(t, batch, latest)

	buffer.restore(batch)

	takeBufferNotification(t, buffer)
	assertQuoteBatch(t, buffer.take(), latest)
	select {
	case <-buffer.updates:
		t.Fatal("restoring one batch accumulated extra notifications")
	default:
	}
}

func TestQuoteBufferRestorePreservesPendingVersions(t *testing.T) {
	for _, restoredInvalidated := range []bool{false, true} {
		for _, pendingInvalidated := range []bool{false, true} {
			for _, relation := range []string{"older", "equal", "newer"} {
				name := fmt.Sprintf("restored_%v/pending_%v/%s", restoredInvalidated, pendingInvalidated, relation)
				t.Run(name, func(t *testing.T) {
					buffer := newQuoteBuffer()
					at := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
					restored := bufferTestQuote("ETH", "Binance", "99", at, restoredInvalidated)
					neighbor := bufferTestQuote("XAUT", "Binance", "98", at, true)
					buffer.put(restored)
					buffer.put(neighbor)
					takeBufferNotification(t, buffer)
					batch := buffer.take()
					pendingAt := at
					switch relation {
					case "older":
						pendingAt = at.Add(-time.Second)
					case "newer":
						pendingAt = at.Add(time.Second)
					}
					pending := bufferTestQuote("ETH", "Binance", "103", pendingAt, pendingInvalidated)
					pending.SourceAt = at.Add(-2 * time.Second)
					other := bufferTestQuote("ETH", "Bybit", "104", at, false)
					buffer.put(pending)
					buffer.put(other)

					buffer.restore(batch)

					want := pending
					if relation == "older" {
						want = restored
					}
					takeBufferNotification(t, buffer)
					assertQuoteBatch(t, buffer.take(), want, neighbor, other)
					assertQuoteBatch(t, batch, restored, neighbor)
				})
			}
		}
	}
}

func TestQuoteBufferConcurrentProducersAndConsumer(t *testing.T) {
	buffer := newQuoteBuffer()
	at := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	pairs := []bufferTestPair{
		{
			coin:     "ETH",
			exchange: "Binance",
		},
		{
			coin:     "ETH",
			exchange: "Bybit",
		},
		{
			coin:     "XAUT",
			exchange: "Binance",
		},
		{
			coin:     "XAUT",
			exchange: "Bybit",
		},
	}
	finished := make(chan struct{})
	consumed := make(chan []contracts.Quote, 1)

	go func() {
		latest := make(map[bufferTestPair]contracts.Quote)
		merge := func(batch []contracts.Quote) {
			for _, quote := range batch {
				latest[bufferTestPair{
					coin:     quote.Coin,
					exchange: quote.Exchange,
				}] = quote
			}
		}

		for {
			select {
			case <-buffer.updates:
				merge(buffer.take())
			case <-finished:
				merge(buffer.take())
				var quotes []contracts.Quote
				for _, quote := range latest {
					quotes = append(quotes, quote)
				}

				consumed <- quotes

				return
			}
		}
	}()

	var producers sync.WaitGroup
	for _, pair := range pairs {
		producers.Go(func() {
			for index := range 64 {
				buffer.put(bufferTestQuote(pair.coin, pair.exchange, fmt.Sprint(index), at.Add(time.Duration(index)*time.Millisecond), index%2 == 1))
			}
		})
	}

	producers.Wait()
	close(finished)
	var expected []contracts.Quote
	for _, pair := range pairs {
		expected = append(expected, bufferTestQuote(pair.coin, pair.exchange, "63", at.Add(63*time.Millisecond), true))
	}

	assertQuoteBatch(t, <-consumed, expected...)
	assertQuoteBatch(t, buffer.take())
}
