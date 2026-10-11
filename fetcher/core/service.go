package core

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type FetcherService struct {
	exchanges []Exchange
	store     QuoteStore
}

func NewFetcherService(exchanges []Exchange, store QuoteStore) *FetcherService {
	return &FetcherService{
		exchanges: exchanges,
		store:     store,
	}
}

func (s *FetcherService) Run(ctx context.Context, coins []string) {
	var workers sync.WaitGroup

	buffer := newQuoteBuffer()

	workers.Go(func() {
		s.runWriter(ctx, buffer)
	})

	onQuote := func(quote contracts.Quote) error {
		buffer.put(quote)

		slog.Debug("quote_buffered", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "invalidated", quote.Invalidated, "received_at", quote.ReceivedAt)

		return ctx.Err()
	}

	for _, exchange := range s.exchanges {
		workers.Go(func() {
			s.runExchange(ctx, exchange, coins, onQuote)
		})
	}

	workers.Wait()
}

func (s *FetcherService) runWriter(ctx context.Context, buffer *quoteBuffer) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-buffer.updates:
		}

		quotes := buffer.take()
		if len(quotes) == 0 {
			continue
		}

		startedAt := time.Now()

		if err := s.store.SaveBatch(ctx, quotes); err != nil {
			if ctx.Err() != nil {
				return
			}

			slog.Error("redis_write_failed", "component", "core", "batch_size", len(quotes), "duration", time.Since(startedAt), "error", err)

			buffer.restore(quotes)

			slog.Debug("quote_batch_restored", "component", "core", "batch_size", len(quotes), "retry_after", 100*time.Millisecond)

			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		} else {
			slog.Debug("quote_batch_delivered", "component", "core", "batch_size", len(quotes), "duration", time.Since(startedAt))
		}
	}
}

func (s *FetcherService) runExchange(ctx context.Context, exchange Exchange, coins []string, onQuote func(contracts.Quote) error) {
	failures := 0

	for ctx.Err() == nil {
		slog.Info("stream_starting", "component", "core", "exchange", exchange.Name(), "coins", coins, "failures", failures)

		err := exchange.Stream(ctx, coins, func(quote contracts.Quote) error {
			if failures > 0 {
				slog.Info("exchange_recovered", "component", "core", "exchange", exchange.Name(), "previous_failures", failures)
			}

			failures = 0

			return onQuote(quote)
		})

		if ctx.Err() != nil {
			slog.Info("stream_stopped", "component", "core", "exchange", exchange.Name(), "reason", "context_cancelled")

			return
		}

		failures++

		if failures >= 5 {
			slog.Error("exchange_disabled", "component", "core", "exchange", exchange.Name(), "failures", failures, "error", err)

			return
		}

		retryAfter := time.Duration(failures) * 500 * time.Millisecond

		slog.Warn("stream_failed", "component", "core", "exchange", exchange.Name(), "failures", failures, "retry_after", retryAfter, "error", err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(retryAfter):
		}
	}
}
