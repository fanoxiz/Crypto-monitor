package contracts

import "github.com/fanoxiz/crypto-monitor/contracts/grpcpb"

func ToProtoCoin(coin string) grpcpb.CoinName {
	switch coin {
	case "BTC":
		return grpcpb.CoinName_COIN_BTC
	case "ETH":
		return grpcpb.CoinName_COIN_ETH
	case "XAUT":
		return grpcpb.CoinName_COIN_XAUT
	default:
		return grpcpb.CoinName_COIN_UNSPECIFIED
	}
}

func ToProtoExchange(exchange string) grpcpb.ExchangeName {
	switch exchange {
	case "Binance":
		return grpcpb.ExchangeName_EXCHANGE_BINANCE
	case "Bitget":
		return grpcpb.ExchangeName_EXCHANGE_BITGET
	case "Bybit":
		return grpcpb.ExchangeName_EXCHANGE_BYBIT
	case "OKX":
		return grpcpb.ExchangeName_EXCHANGE_OKX
	default:
		return grpcpb.ExchangeName_EXCHANGE_UNSPECIFIED
	}
}

func FromProtoCoin(coin grpcpb.CoinName) string {
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

func FromProtoExchange(exchange grpcpb.ExchangeName) string {
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
