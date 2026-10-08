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

		for _, deal := range a.FindDeals(quotes, time.Now().UTC()) {
			if ctx.Err() != nil {
				return
			}

			if err := a.sender.Send(ctx, deal); err != nil {
				if ctx.Err() != nil {
					return
				}

				log.Printf("level=ERROR component=core event=deal_send_failed coin=%s err=%q", deal.Coin, err)
				continue
			}

			log.Printf("level=DEBUG component=core event=deal_sent coin=%s buy_exchange=%s sell_exchange=%s profit_usdt=%.8g",
				deal.Coin,
				deal.BuyExchange,
				deal.SellExchange,
				deal.ProfitUSDT,
			)
		}
	}
}

func (a *AnalyzerService) FindDeals(quotes []contracts.Quote, now time.Time) []contracts.ProfitDealInfo {
	prices := make(map[string][]marketQuote)

	for _, quote := range quotes {
		price, valid := a.parseQuote(quote, now)
		if valid {
			prices[quote.Coin] = append(prices[quote.Coin], price)
		}
	}

	var deals []contracts.ProfitDealInfo

	for coin, exchanges := range prices {
		var best contracts.ProfitDealInfo
		var bestQuantity float64

		for _, buy := range exchanges {
			for _, sell := range exchanges {
				if buy.exchange == sell.exchange || sell.sellProceeds <= buy.buyCost {
					continue
				}

				quantity := math.Min(buy.askSize, sell.bidSize)
				if quantity <= bestQuantity {
					continue
				}

				buyAmount := quantity * buy.buyCost
				sellAmount := quantity * sell.sellProceeds
				profit := sellAmount - buyAmount

				if !positiveFinite(buyAmount) || !positiveFinite(sellAmount) || !positiveFinite(profit) {
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
				bestQuantity = quantity
			}
		}

		if bestQuantity > 0 {
			deals = append(deals, best)
		}
	}

	return deals
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
