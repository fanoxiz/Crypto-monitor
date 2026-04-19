package core

import (
	"context"
	"math"

	"github.com/fanoxiz/crypto-monitor/contracts" // allowed core dependency
)

type ExecutorService struct {
	repo           DBRepository
	tradeSize      float64
	initialBalance float64
}

func NewExecutorService(repo DBRepository, tradeSize float64, initialBalance float64) *ExecutorService {
	return &ExecutorService{
		repo:           repo,
		tradeSize:      tradeSize,
		initialBalance: initialBalance,
	}
}

func (s *ExecutorService) ProcessDeal(ctx context.Context, deal contracts.ProfitDealInfo) error {
	earned := s.tradeSize * (deal.ProfitPercent / 100.0)
	return s.repo.SaveDealAndUpdateBalance(ctx, deal, earned)
}

func (s *ExecutorService) GetCurrentBalance(ctx context.Context) (float64, error) {
	return s.repo.GetBalance(ctx)
}

func (s *ExecutorService) GetStats(ctx context.Context) (DealStats, error) {
	balance, err := s.repo.GetBalance(ctx)
	if err != nil {
		return DealStats{}, err
	}

	dealsCount, err := s.repo.GetDealsCount(ctx)
	if err != nil {
		return DealStats{}, err
	}

	return DealStats{
		CurrentBalance: math.Round(balance*100) / 100,
		TotalEarned:    math.Round((balance-s.initialBalance)*100) / 100,
		DealsCount:     dealsCount,
	}, nil
}
