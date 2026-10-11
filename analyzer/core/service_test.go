package core

import (
	"context"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type testQuoteStore struct {
	stream func(context.Context, func(contracts.Quote)) error
}

func (s testQuoteStore) Stream(ctx context.Context, handle func(contracts.Quote)) error {
	return s.stream(ctx, handle)
}

type testDealSender struct {
	deals []contracts.ProfitDealInfo
	sent  chan struct{}
}

func (s *testDealSender) Send(_ context.Context, deal contracts.ProfitDealInfo) error {
	s.deals = append(s.deals, deal)

	select {
	case s.sent <- struct{}{}:
	default:
	}

	return nil
}

func testQuote(exchange, bid, ask string, receivedAt time.Time) contracts.Quote {
	return contracts.Quote{
		Coin:       "BTC",
		Exchange:   exchange,
		BidPrice:   bid,
		BidSize:    "1",
		AskPrice:   ask,
		AskSize:    "1",
		ReceivedAt: receivedAt,
	}
}

func TestHandleQuoteKeepsNewerStateAndSuppressesRepeatedDeals(t *testing.T) {
	ctx := t.Context()
	now := time.Now().UTC()
	quotes := []contracts.Quote{
		testQuote("A", "99", "100", now),
		testQuote("B", "110", "111", now),
		testQuote("A", "49", "50", now.Add(-time.Second)),
		testQuote("B", "110", "111", now),
	}
	service := NewAnalyzerService(map[string]float64{"A": 0, "B": 0}, nil, nil, 500*time.Millisecond)
	prices := make(map[string][]marketQuote)
	fingerprints := make(map[string]dealFingerprint)
	deals := make(chan contracts.ProfitDealInfo, dealQueueCapacity)

	for _, quote := range quotes {
		service.handleQuote(ctx, quote, prices, deals, fingerprints)
	}

	if len(deals) != 1 {
		t.Fatalf("expected one queued deal after duplicate suppression, got %d", len(deals))
	}

	deal := <-deals
	if deal.BuyExchange != "A" || deal.SellExchange != "B" || deal.ProfitUSDT != 10 {
		t.Fatalf("older quote replaced current state: %+v", deal)
	}
}

func TestStreamChecksAgeOfCachedCounterpart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	store := testQuoteStore{
		stream: func(ctx context.Context, handle func(contracts.Quote)) error {
			handle(testQuote("A", "99", "100", time.Now().UTC()))

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}

			handle(testQuote("B", "110", "111", time.Now().UTC()))
			cancel()

			return ctx.Err()
		},
	}
	sender := &testDealSender{}
	service := NewAnalyzerService(map[string]float64{"A": 0, "B": 0}, store, sender, 50*time.Millisecond)

	service.Run(ctx)

	if len(sender.deals) != 0 {
		t.Fatalf("expired counterpart produced deals: %+v", sender.deals)
	}
}

func TestStreamOnlyRecalculatesUpdatedCoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	now := time.Now().UTC()
	other := testQuote("A", "99", "100", now)
	other.Coin = "ETH"
	sender := &testDealSender{
		sent: make(chan struct{}, 1),
	}
	store := testQuoteStore{
		stream: func(ctx context.Context, handle func(contracts.Quote)) error {
			handle(testQuote("A", "99", "100", now))
			handle(testQuote("B", "110", "111", now))
			handle(other)

			select {
			case <-sender.sent:
			case <-ctx.Done():
				return ctx.Err()
			}

			cancel()

			return ctx.Err()
		},
	}
	service := NewAnalyzerService(map[string]float64{"A": 0, "B": 0}, store, sender, 500*time.Millisecond)

	service.Run(ctx)

	if len(sender.deals) != 1 || sender.deals[0].Coin != "BTC" {
		t.Fatalf("unrelated update recalculated BTC: %+v", sender.deals)
	}
}
