package sender

import (
	"context"
	"fmt"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"google.golang.org/grpc"
)

type SenderService struct {
	client grpcpb.AnalyzerServiceClient
}

func NewSenderService(conn *grpc.ClientConn) *SenderService {
	return &SenderService{
		client: grpcpb.NewAnalyzerServiceClient(conn),
	}
}

func (s *SenderService) Send(msg contracts.MarketTickerInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pbMsg := toProtoPrice(msg)

	_, err := s.client.ProcessPrices(ctx, pbMsg)
	if err != nil {
		return fmt.Errorf("send to analyzer: %w", err)
	}
	return nil
}

func toProtoPrice(msg contracts.MarketTickerInfo) *grpcpb.MarketTickerInfo {
	return &grpcpb.MarketTickerInfo{
		CoinName:     toProtoCoin(msg.CoinName),
		ExchangeName: toProtoExchange(msg.ExchangeName),
		Price: &grpcpb.BidAsk{
			Bid: msg.Price.Bid,
			Ask: msg.Price.Ask,
		},
	}
}

func toProtoCoin(coin string) grpcpb.CoinName {
	switch coin {
	case "BTC":
		return grpcpb.CoinName_COIN_BTC
	case "ETH":
		return grpcpb.CoinName_COIN_ETH
	case "XAUt":
		return grpcpb.CoinName_COIN_XAUT
	default:
		return grpcpb.CoinName_COIN_UNSPECIFIED
	}
}

func toProtoExchange(exchange string) grpcpb.ExchangeName {
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
