package core

import (
	"context"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type QuoteStore interface {
	GetQuotes(ctx context.Context) ([]contracts.Quote, error)
}
