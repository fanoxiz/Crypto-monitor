package api

import (
	"log/slog"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

func quoteFromBook(coin, exchange string, bids, asks [][]string) contracts.Quote {
	if coin == "" || bids == nil || asks == nil {
		slog.Debug("book_message_ignored", "component", "api", "coin", coin, "exchange", exchange, "reason", "missing_identity_or_sides")

		return contracts.Quote{}
	}

	if len(bids) == 0 || len(asks) == 0 {
		return contracts.Quote{
			Coin:        coin,
			Exchange:    exchange,
			Invalidated: true,
		}
	}

	if len(bids[0]) < 2 || len(asks[0]) < 2 {
		slog.Debug("book_message_ignored", "component", "api", "coin", coin, "exchange", exchange, "reason", "malformed_level", "bid_fields", len(bids[0]), "ask_fields", len(asks[0]))

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
