package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

var ErrInvalidDeal = errors.New("invalid deal")

type ExecutorService struct {
	repo DBRepository
}

func NewExecutorService(repo DBRepository) *ExecutorService {
	return &ExecutorService{
		repo: repo,
	}
}

func (s *ExecutorService) ProcessDeal(ctx context.Context, deal contracts.ProfitDealInfo) error {
	if strings.TrimSpace(deal.Coin) == "" ||
		strings.TrimSpace(deal.BuyExchange) == "" ||
		strings.TrimSpace(deal.SellExchange) == "" ||
		deal.BuyExchange == deal.SellExchange ||
		deal.DetectedAt.IsZero() {
		return fmt.Errorf("%w: coin, different exchanges and detection time are required", ErrInvalidDeal)
	}

	for _, amount := range []float64{deal.BuyAmountUSDT, deal.SellAmountUSDT, deal.ProfitUSDT} {
		if amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
			return fmt.Errorf("%w: amounts must be finite and positive", ErrInvalidDeal)
		}
	}

	difference := deal.SellAmountUSDT - deal.BuyAmountUSDT
	if difference <= 0 || math.Abs(difference-deal.ProfitUSDT) > 1e-9*math.Max(1, math.Abs(difference)) {
		return fmt.Errorf("%w: profit must equal sale minus purchase", ErrInvalidDeal)
	}

	return s.repo.SaveDeal(ctx, deal)
}
