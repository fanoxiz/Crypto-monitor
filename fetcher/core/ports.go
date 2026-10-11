package core

import (
	"context"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type Exchange interface {
	Name() string
	Stream(ctx context.Context, coins []string, save func(contracts.Quote) error, onHealthy func()) error
}

type QuoteStore interface {
	SaveBatch(ctx context.Context, quotes []contracts.Quote) error
}
