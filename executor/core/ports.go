package core

import (
	"context"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type DealStats struct {
	CurrentBalance float64 `json:"current_balance"`
	TotalEarned    float64 `json:"total_earned"`
	DealsCount     int64   `json:"deals_count"`
}

type Executor interface {
	ProcessDeal(ctx context.Context, msg contracts.ProfitDealInfo) error
	GetStats(ctx context.Context) (DealStats, error)
}

type DBRepository interface {
	SaveDealAndUpdateBalance(ctx context.Context, deal contracts.ProfitDealInfo, earnedUSD float64) error
	GetBalance(ctx context.Context) (float64, error)
	GetDealsCount(ctx context.Context) (int64, error)
}
