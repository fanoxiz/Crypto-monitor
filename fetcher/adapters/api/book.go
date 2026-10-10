package api

import "github.com/fanoxiz/crypto-monitor/contracts"

func quoteFromBook(coin, exchange string, bids, asks [][]string) contracts.Quote {
	if len(bids) == 0 || len(asks) == 0 || len(bids[0]) < 2 || len(asks[0]) < 2 {
		return contracts.Quote{}
	}

	return contracts.Quote{
		Coin:     coin,
		Exchange: exchange,
		BidPrice: bids[0][0],
		BidSize:  bids[0][1],
		AskPrice: asks[0][0],
		AskSize:  asks[0][1],
	}
}
