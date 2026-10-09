package core

import (
	"context"
	"log"
	"math/rand/v2"
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

		if err := s.store.SaveBatch(ctx, quotes); err != nil {
			if ctx.Err() != nil {
				return
			}

			log.Printf("level=ERROR component=core event=redis_write_failed err=%q", err)

			buffer.restore(quotes)

			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

func (s *FetcherService) runExchange(ctx context.Context, exchange Exchange, coins []string, onQuote func(contracts.Quote) error) {
	delay := time.Second

	for ctx.Err() == nil {
		received := false

		log.Printf("level=INFO component=core event=stream_starting exchange=%s", exchange.Name())

		err := exchange.Stream(ctx, coins, func(quote contracts.Quote) error {
			received = true

			return onQuote(quote)
		})

		if ctx.Err() != nil {
			return
		}

		if received {
			delay = time.Second
		}

		retryAfter := delay/2 + time.Duration(rand.Int64N(int64(delay/2)))

		log.Printf("level=ERROR component=core event=stream_failed exchange=%s retry_after=%s err=%q", exchange.Name(), retryAfter, err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(retryAfter):
		}

		delay = min(2*delay, 10*time.Second)
	}
}
