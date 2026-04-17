package core

import (
	"log"

	"github.com/fanoxiz/crypto-monitor/contracts" // allowed core dependency
)

type ExecutorService struct{}

func NewExecutorService() *ExecutorService {
	return &ExecutorService{}
}

func (s *ExecutorService) ProcessDeal(msg contracts.ProfitDealInfo) error {
	log.Printf(
		"[%s] buy=%.2f (%s) | sell=%.2f(%s) | profit=%.3f%% ($%.2f)",
		msg.CoinName,
		msg.AskPrice,
		msg.AskExchange,
		msg.BidPrice,
		msg.BidExchange,
		msg.ProfitPercent,
		msg.ProfitAbs,
	)

	return nil
}
