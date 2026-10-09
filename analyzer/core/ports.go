package core

import (
	"context"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type QuoteStore interface {
	Stream(ctx context.Context, handle func(contracts.Quote)) error
}

type DealSender interface {
	Send(ctx context.Context, deal contracts.ProfitDealInfo) error
}
