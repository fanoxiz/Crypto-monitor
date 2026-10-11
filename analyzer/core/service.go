package core

import (
	"context"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type AnalyzerService struct {
	fees        map[string]float64
	store       QuoteStore
	sender      DealSender
	maxQuoteAge time.Duration
}

func NewAnalyzerService(fees map[string]float64, store QuoteStore, sender DealSender, maxQuoteAge time.Duration) *AnalyzerService {
	return &AnalyzerService{
		fees:        fees,
		store:       store,
		sender:      sender,
		maxQuoteAge: maxQuoteAge,
	}
}

func (a *AnalyzerService) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	deals := make(chan contracts.ProfitDealInfo, dealQueueCapacity)
	senderDone := make(chan struct{})

	go func() {
		defer close(senderDone)

		a.runSender(ctx, deals)
	}()

	defer func() {
		cancel()
		<-senderDone
	}()

	for ctx.Err() == nil {
		prices := make(map[string][]marketQuote)

		slog.Info("quote_stream_starting", "component", "core")

		err := a.store.Stream(ctx, func(quote contracts.Quote) {
			a.handleQuote(ctx, quote, prices, deals)
		})

		if ctx.Err() != nil {
			return
		}

		slog.Error("quote_stream_failed", "component", "core", "retry_after", time.Second, "error", err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (a *AnalyzerService) handleQuote(ctx context.Context, quote contracts.Quote, prices map[string][]marketQuote, deals chan contracts.ProfitDealInfo) {
	if ctx.Err() != nil {
		return
	}

	now := time.Now().UTC()

	slog.Debug("quote_processing", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "source_at", quote.SourceAt, "expires_at", quote.ExpiresAt, "age", now.Sub(quote.ReceivedAt), "invalidated", quote.Invalidated)

	exchanges := prices[quote.Coin]
	index := -1

	for i, price := range exchanges {
		if price.exchange == quote.Exchange {
			if quote.ReceivedAt.Before(price.receivedAt) {
				slog.Debug("quote_ignored", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "older_update", "received_at", quote.ReceivedAt, "current_received_at", price.receivedAt)

				return
			}

			index = i
			break
		}
	}

	price, valid := a.parseQuote(quote, now)

	if valid {
		if index < 0 {
			exchanges = append(exchanges, price)
		} else {
			exchanges[index] = price
		}

		slog.Debug("quote_accepted", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "buy_cost", price.buyCost, "sell_proceeds", price.sellProceeds, "bid_size", price.bidSize, "ask_size", price.askSize)
	} else if index >= 0 {
		exchanges = append(exchanges[:index], exchanges[index+1:]...)

		slog.Debug("quote_removed", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "invalidated", quote.Invalidated)
	}

	current := exchanges[:0]

	for _, price := range exchanges {
		if a.fresh(price.receivedAt, price.expiresAt, now) {
			current = append(current, price)
		} else {
			slog.Debug("cached_quote_expired", "component", "core", "coin", quote.Coin, "exchange", price.exchange, "expires_at", price.expiresAt, "age", now.Sub(price.receivedAt))
		}
	}

	prices[quote.Coin] = current

	deal, exists := a.findDeal(quote.Coin, current, now)
	if !exists {
		slog.Debug("deal_not_found", "component", "core", "coin", quote.Coin, "available_exchanges", len(current))

		return
	}

	a.enqueueDeal(ctx, deals, deal)
}

func (a *AnalyzerService) findDeal(coin string, exchanges []marketQuote, now time.Time) (contracts.ProfitDealInfo, bool) {
	var best contracts.ProfitDealInfo

	for _, buy := range exchanges {
		for _, sell := range exchanges {
			if buy.exchange == sell.exchange || sell.sellProceeds <= buy.buyCost {
				slog.Debug("deal_pair_skipped", "component", "core", "coin", coin, "buy_exchange", buy.exchange, "sell_exchange", sell.exchange, "reason", "same_exchange_or_unprofitable", "buy_cost", buy.buyCost, "sell_proceeds", sell.sellProceeds)

				continue
			}

			quantity := math.Min(buy.askSize, sell.bidSize)
			buyAmount := quantity * buy.buyCost
			sellAmount := quantity * sell.sellProceeds
			profit := sellAmount - buyAmount

			if !positiveFinite(buyAmount) || !positiveFinite(sellAmount) || !positiveFinite(profit) {
				slog.Debug("deal_pair_skipped", "component", "core", "coin", coin, "buy_exchange", buy.exchange, "sell_exchange", sell.exchange, "reason", "invalid_amounts", "buy_usdt", buyAmount, "sell_usdt", sellAmount, "profit_usdt", profit)

				continue
			}

			if profit <= best.ProfitUSDT {
				slog.Debug("deal_pair_skipped", "component", "core", "coin", coin, "buy_exchange", buy.exchange, "sell_exchange", sell.exchange, "reason", "profit_not_better", "profit_usdt", profit, "best_profit_usdt", best.ProfitUSDT)

				continue
			}

			best = contracts.ProfitDealInfo{
				Coin:           coin,
				BuyExchange:    buy.exchange,
				SellExchange:   sell.exchange,
				BuyAmountUSDT:  buyAmount,
				SellAmountUSDT: sellAmount,
				ProfitUSDT:     profit,
				DetectedAt:     now,
			}

			slog.Debug("deal_candidate_selected", "component", "core", "coin", coin, "buy_exchange", buy.exchange, "sell_exchange", sell.exchange, "quantity", quantity, "buy_usdt", buyAmount, "sell_usdt", sellAmount, "profit_usdt", profit)
		}
	}

	return best, best.ProfitUSDT > 0
}

func (a *AnalyzerService) parseQuote(quote contracts.Quote, now time.Time) (marketQuote, bool) {
	if quote.Invalidated {
		slog.Debug("quote_rejected", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "invalidation")

		return marketQuote{}, false
	}

	fee, exists := a.fees[quote.Exchange]
	if !exists || math.IsNaN(fee) || math.IsInf(fee, 0) || fee < 0 || fee >= 1 || quote.Coin == "" {
		slog.Debug("quote_rejected", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "invalid_identity_or_fee", "fee_configured", exists, "fee", fee)

		return marketQuote{}, false
	}

	if !a.fresh(quote.ReceivedAt, quote.ExpiresAt, now) {
		slog.Debug("quote_rejected", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "invalid_age", "source_at", quote.SourceAt, "received_at", quote.ReceivedAt, "expires_at", quote.ExpiresAt, "age", now.Sub(quote.ReceivedAt), "max_quote_age", a.maxQuoteAge)

		return marketQuote{}, false
	}

	raw := [4]string{
		quote.BidPrice,
		quote.AskPrice,
		quote.BidSize,
		quote.AskSize,
	}
	var values [4]float64

	for i, value := range raw {
		number, err := strconv.ParseFloat(value, 64)
		if err != nil || !positiveFinite(number) {
			fields := [4]string{
				"bid_price",
				"ask_price",
				"bid_size",
				"ask_size",
			}

			slog.Debug("quote_rejected", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "invalid_number", "field", fields[i], "value", value, "error", err)

			return marketQuote{}, false
		}

		values[i] = number
	}

	if values[0] > values[1] {
		slog.Debug("quote_rejected", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "crossed_book", "bid_price", values[0], "ask_price", values[1])

		return marketQuote{}, false
	}

	price := marketQuote{
		exchange:     quote.Exchange,
		askSize:      values[3],
		bidSize:      values[2],
		buyCost:      values[1] * (1 + fee),
		sellProceeds: values[0] * (1 - fee),
		receivedAt:   quote.ReceivedAt,
		expiresAt:    quote.ExpiresAt,
	}

	if !positiveFinite(price.buyCost) || !positiveFinite(price.sellProceeds) {
		slog.Debug("quote_rejected", "component", "core", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "invalid_fee_adjusted_prices", "buy_cost", price.buyCost, "sell_proceeds", price.sellProceeds, "fee", fee)

		return marketQuote{}, false
	}

	return price, true
}

func (a *AnalyzerService) fresh(receivedAt, expiresAt, now time.Time) bool {
	age := now.Sub(receivedAt)

	return !receivedAt.IsZero() && age >= 0 && age <= a.maxQuoteAge && (expiresAt.IsZero() || now.Before(expiresAt))
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
