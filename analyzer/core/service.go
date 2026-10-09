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
		prices := make(map[string][]marketQuote)

		err := a.store.Stream(ctx, func(quote contracts.Quote) {
			if ctx.Err() != nil {
				return
			}

			now := time.Now().UTC()
			exchanges := prices[quote.Coin]
			index := -1

			for i, price := range exchanges {
				if price.exchange == quote.Exchange {
					if quote.ReceivedAt.Before(price.receivedAt) {
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
			} else if index >= 0 {
				exchanges = append(exchanges[:index], exchanges[index+1:]...)
			}

			current := exchanges[:0]

			for _, price := range exchanges {
				if a.fresh(price.receivedAt, now) {
					current = append(current, price)
				}
			}

			prices[quote.Coin] = current

			deal, exists := a.findDeal(quote.Coin, current, now)
			if !exists {
				return
			}

			if err := a.sender.Send(ctx, deal); err != nil {
				if ctx.Err() != nil {
					return
				}

				log.Printf("level=ERROR component=core event=deal_send_failed coin=%s err=%q", deal.Coin, err)
				return
			}

			log.Printf("level=DEBUG component=core event=deal_sent coin=%s buy_exchange=%s sell_exchange=%s profit_usdt=%.8g",
				deal.Coin,
				deal.BuyExchange,
				deal.SellExchange,
				deal.ProfitUSDT,
			)
		})

		if ctx.Err() != nil {
			return
		}

		log.Printf("level=ERROR component=core event=quote_stream_failed err=%q", err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
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
		if deal, exists := a.findDeal(coin, exchanges, now); exists {
			deals = append(deals, deal)
		}
	}

	return deals
}

func (a *AnalyzerService) findDeal(coin string, exchanges []marketQuote, now time.Time) (contracts.ProfitDealInfo, bool) {
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

	return best, bestQuantity > 0
}

func (a *AnalyzerService) parseQuote(quote contracts.Quote, now time.Time) (marketQuote, bool) {
	fee, exists := a.fees[quote.Exchange]
	if !exists || math.IsNaN(fee) || math.IsInf(fee, 0) || fee < 0 || fee >= 1 || quote.Coin == "" {
		return marketQuote{}, false
	}

	if !a.fresh(quote.ReceivedAt, now) {
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
		receivedAt:   quote.ReceivedAt,
	}

	if !positiveFinite(price.buyCost) || !positiveFinite(price.sellProceeds) {
		return marketQuote{}, false
	}

	return price, true
}

func (a *AnalyzerService) fresh(receivedAt, now time.Time) bool {
	age := now.Sub(receivedAt)

	return !receivedAt.IsZero() && age >= 0 && age <= a.maxQuoteAge
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
