package core

import (
	"sync"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type quoteBuffer struct {
	mutex   sync.Mutex
	pending map[string]contracts.Quote
	updates chan struct{}
}

func newQuoteBuffer() *quoteBuffer {
	return &quoteBuffer{
		pending: make(map[string]contracts.Quote),
		updates: make(chan struct{}, 1),
	}
}

func (b *quoteBuffer) put(quote contracts.Quote) {
	b.mutex.Lock()
	b.pending[contracts.QuoteKey(quote.Coin, quote.Exchange)] = quote
	b.mutex.Unlock()

	b.notify()
}

func (b *quoteBuffer) take() []contracts.Quote {
	b.mutex.Lock()
	quotes := make([]contracts.Quote, 0, len(b.pending))

	for _, quote := range b.pending {
		quotes = append(quotes, quote)
	}

	clear(b.pending)
	b.mutex.Unlock()

	return quotes
}

func (b *quoteBuffer) restore(quotes []contracts.Quote) {
	b.mutex.Lock()

	for _, quote := range quotes {
		key := contracts.QuoteKey(quote.Coin, quote.Exchange)
		current, exists := b.pending[key]
		if !exists || current.ReceivedAt.Before(quote.ReceivedAt) {
			b.pending[key] = quote
		}
	}

	b.mutex.Unlock()

	b.notify()
}

func (b *quoteBuffer) notify() {
	select {
	case b.updates <- struct{}{}:
	default:
	}
}
