package core

import (
	"context"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type RecentDeal struct {
	Coin          string    `json:"coin"`
	BuyExchange   string    `json:"buy_exchange"`
	SellExchange  string    `json:"sell_exchange"`
	ProfitPercent float64   `json:"profit_percent"`
	EarnedUSD     float64   `json:"earned_usd"`
	CreatedAt     time.Time `json:"created_at"`
}

type DealStats struct {
	CurrentBalance float64      `json:"current_balance"`
	TotalEarned    float64      `json:"total_earned"`
	DealsCount     int64        `json:"deals_count"`
	RecentDeals    []RecentDeal `json:"recent_deals"`
}

type Executor interface {
	ProcessDeal(ctx context.Context, msg contracts.ProfitDealInfo) error
	GetStats(ctx context.Context) (DealStats, error)
}

type DBRepository interface {
	SaveDealAndUpdateBalance(ctx context.Context, deal contracts.ProfitDealInfo, earnedUSD float64) error
	GetBalance(ctx context.Context) (float64, error)
	GetDealsCount(ctx context.Context) (int64, error)
	GetRecentDeals(ctx context.Context, limit int) ([]RecentDeal, error)
}
