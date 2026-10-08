package core

import (
	"context"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type Executor interface {
	ProcessDeal(ctx context.Context, deal contracts.ProfitDealInfo) error
}

type DBRepository interface {
	SaveDeal(ctx context.Context, deal contracts.ProfitDealInfo) error
}
