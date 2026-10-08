package core

type Opportunity struct {
	Coin         string
	BuyExchange  string
	SellExchange string

	AskPrice                 float64
	BidPrice                 float64
	Quantity                 float64
	BuyAmountUSDT            float64
	BuyCostWithFeeUSDT       float64
	SellProceedsAfterFeeUSDT float64
	DifferenceUSDT           float64
	SpreadPercent            float64
}
