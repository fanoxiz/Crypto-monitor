package core

import (
	"context"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type AnalyzerService struct {
	fees        map[string]float64
	store       QuoteStore
	maxQuoteAge time.Duration
}

func NewAnalyzerService(fees map[string]float64, store QuoteStore, maxQuoteAge time.Duration) *AnalyzerService {
	return &AnalyzerService{
		fees:        fees,
		store:       store,
		maxQuoteAge: maxQuoteAge,
	}
}

func (a *AnalyzerService) Run(ctx context.Context) {
	for ctx.Err() == nil {
		quotes, err := a.store.GetQuotes(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			log.Printf("level=ERROR component=core event=redis_read_failed err=%q", err)

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}

			continue
		}

		for _, opportunity := range a.FindOpportunities(quotes, time.Now().UTC()) {
			log.Printf("level=DEBUG component=core event=opportunity coin=%s buy_exchange=%s sell_exchange=%s quantity=%.8g buy_usdt=%.8g cost_with_fee_usdt=%.8g proceeds_after_fee_usdt=%.8g difference_usdt=%.8g spread_percent=%.8g",
				opportunity.Coin,
				opportunity.BuyExchange,
				opportunity.SellExchange,
				opportunity.Quantity,
				opportunity.BuyAmountUSDT,
				opportunity.BuyCostWithFeeUSDT,
				opportunity.SellProceedsAfterFeeUSDT,
				opportunity.DifferenceUSDT,
				opportunity.SpreadPercent,
			)
		}
	}
}

type marketQuote struct {
	exchange     string
	askPrice     float64
	bidPrice     float64
	askSize      float64
	bidSize      float64
	buyCost      float64
	sellProceeds float64
}

func (a *AnalyzerService) FindOpportunities(quotes []contracts.Quote, now time.Time) []Opportunity {
	prices := make(map[string][]marketQuote)

	for _, quote := range quotes {
		price, valid := a.parseQuote(quote, now)
		if valid {
			prices[quote.Coin] = append(prices[quote.Coin], price)
		}
	}

	var opportunities []Opportunity

	for coin, exchanges := range prices {
		var best Opportunity

		for _, buy := range exchanges {
			for _, sell := range exchanges {
				if buy.exchange == sell.exchange {
					continue
				}

				spread := sell.sellProceeds - buy.buyCost
				if spread <= 0 {
					continue
				}

				quantity := math.Min(buy.askSize, sell.bidSize)
				if quantity <= best.Quantity {
					continue
				}

				candidate := Opportunity{
					Coin:                     coin,
					BuyExchange:              buy.exchange,
					SellExchange:             sell.exchange,
					AskPrice:                 buy.askPrice,
					BidPrice:                 sell.bidPrice,
					Quantity:                 quantity,
					BuyAmountUSDT:            quantity * buy.askPrice,
					BuyCostWithFeeUSDT:       quantity * buy.buyCost,
					SellProceedsAfterFeeUSDT: quantity * sell.sellProceeds,
					DifferenceUSDT:           quantity * spread,
					SpreadPercent:            spread / buy.buyCost * 100,
				}

				if !positiveFinite(candidate.BuyAmountUSDT) ||
					 !positiveFinite(candidate.BuyCostWithFeeUSDT) ||
					 !positiveFinite(candidate.SellProceedsAfterFeeUSDT) ||
					 !positiveFinite(candidate.DifferenceUSDT) ||
					 !positiveFinite(candidate.SpreadPercent) {
					continue
				}

				best = candidate
			}
		}

		if best.Quantity > 0 {
			opportunities = append(opportunities, best)
		}
	}

	return opportunities
}

func (a *AnalyzerService) parseQuote(quote contracts.Quote, now time.Time) (marketQuote, bool) {
	fee, exists := a.fees[quote.Exchange]
	if !exists || math.IsNaN(fee) || math.IsInf(fee, 0) || fee < 0 || fee >= 1 || quote.Coin == "" {
		return marketQuote{}, false
	}

	age := now.Sub(quote.ReceivedAt)
	if quote.ReceivedAt.IsZero() || age < 0 || age > a.maxQuoteAge {
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
			return marketQuote{}, false
		}

		values[i] = number
	}

	if values[0] > values[1] {
		return marketQuote{}, false
	}

	price := marketQuote{
		exchange:     quote.Exchange,
		askPrice:     values[1],
		bidPrice:     values[0],
		askSize:      values[3],
		bidSize:      values[2],
		buyCost:      values[1] * (1 + fee),
		sellProceeds: values[0] * (1 - fee),
	}

	if !positiveFinite(price.buyCost) || !positiveFinite(price.sellProceeds) {
		return marketQuote{}, false
	}

	return price, true
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
