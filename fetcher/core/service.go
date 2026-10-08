package core

import (
	"context"
	"log"
	"sync"

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

	for _, exchange := range s.exchanges {
		workers.Go(func() {
			err := exchange.Stream(ctx, coins, func(quote contracts.Quote) error {
				return s.store.Save(ctx, quote)
			})

			if err != nil && ctx.Err() == nil {
				log.Printf("level=ERROR component=core event=stream_failed exchange=%s err=%q", exchange.Name(), err)
			}
		})
	}

	workers.Wait()
}
