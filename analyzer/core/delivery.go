package core

import (
	"context"
	"log/slog"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

const dealQueueCapacity = 128

func (a *AnalyzerService) enqueueDeal(ctx context.Context, deals chan contracts.ProfitDealInfo, deal contracts.ProfitDealInfo) bool {
	for ctx.Err() == nil {
		select {
		case deals <- deal:
			slog.Debug("deal_queued", "component", "core", "coin", deal.Coin, "buy_exchange", deal.BuyExchange, "sell_exchange", deal.SellExchange, "profit_usdt", deal.ProfitUSDT, "queue_size", len(deals))

			return true
		default:
		}

		select {
		case dropped := <-deals:
			slog.Warn("deal_queue_overflow", "component", "core", "coin", dropped.Coin, "buy_exchange", dropped.BuyExchange, "sell_exchange", dropped.SellExchange, "detected_at", dropped.DetectedAt, "reason", "oldest_signal_dropped", "capacity", cap(deals))
		default:
		}
	}

	return false
}

func (a *AnalyzerService) runSender(ctx context.Context, deals <-chan contracts.ProfitDealInfo) {
	slog.Info("deal_sender_started", "component", "core", "capacity", cap(deals))

	defer func() {
		slog.Info("deal_sender_stopped", "component", "core", "discarded_pending", len(deals))
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case deal := <-deals:
			if ctx.Err() != nil {
				return
			}

			startedAt := time.Now()

			slog.Debug("deal_sending", "component", "core", "coin", deal.Coin, "buy_exchange", deal.BuyExchange, "sell_exchange", deal.SellExchange, "buy_usdt", deal.BuyAmountUSDT, "sell_usdt", deal.SellAmountUSDT, "profit_usdt", deal.ProfitUSDT, "detected_at", deal.DetectedAt, "queue_wait", startedAt.Sub(deal.DetectedAt), "queue_size", len(deals))

			if err := a.sender.Send(ctx, deal); err != nil {
				if ctx.Err() != nil {
					return
				}

				slog.Error("deal_send_failed", "component", "core", "coin", deal.Coin, "buy_exchange", deal.BuyExchange, "sell_exchange", deal.SellExchange, "duration", time.Since(startedAt), "error", err)

				continue
			}

			slog.Debug("deal_sent", "component", "core", "coin", deal.Coin, "buy_exchange", deal.BuyExchange, "sell_exchange", deal.SellExchange, "profit_usdt", deal.ProfitUSDT, "duration", time.Since(startedAt))
		}
	}
}
