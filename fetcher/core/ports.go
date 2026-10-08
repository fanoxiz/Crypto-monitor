package core

import (
	"context"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type Exchange interface {
	Name() string
	Stream(ctx context.Context, coins []string, save func(contracts.Quote) error) error
}

type QuoteStore interface {
	Save(ctx context.Context, quote contracts.Quote) error
}
