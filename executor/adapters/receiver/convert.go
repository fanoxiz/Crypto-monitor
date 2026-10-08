package receiver

import "github.com/fanoxiz/crypto-monitor/contracts/grpcpb"

func fromProtoCoin(coin grpcpb.CoinName) string {
	switch coin {
	case grpcpb.CoinName_COIN_BTC:
		return "BTC"
	case grpcpb.CoinName_COIN_ETH:
		return "ETH"
	case grpcpb.CoinName_COIN_XAUT:
		return "XAUT"
	default:
		return ""
	}
}

func fromProtoExchange(exchange grpcpb.ExchangeName) string {
	switch exchange {
	case grpcpb.ExchangeName_EXCHANGE_BINANCE:
		return "Binance"
	case grpcpb.ExchangeName_EXCHANGE_BITGET:
		return "Bitget"
	case grpcpb.ExchangeName_EXCHANGE_BYBIT:
		return "Bybit"
	case grpcpb.ExchangeName_EXCHANGE_OKX:
		return "OKX"
	default:
		return ""
	}
}
