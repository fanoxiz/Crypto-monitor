package sender

import (
	"context"
	"fmt"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SenderService struct {
	client grpcpb.ExecutorServiceClient
}

func NewSenderService(conn *grpc.ClientConn) *SenderService {
	return &SenderService{
		client: grpcpb.NewExecutorServiceClient(conn),
	}
}

func (s *SenderService) Send(msg contracts.ProfitDealInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pbMsg := toProtoDeal(msg)

	_, err := s.client.ProcessDeal(ctx, pbMsg)
	if err != nil {
		return fmt.Errorf("send to executor: %w", err)
	}
	return nil
}

func toProtoDeal(msg contracts.ProfitDealInfo) *grpcpb.ProfitDealInfo {
	return &grpcpb.ProfitDealInfo{
		CoinName:      toProtoCoin(msg.CoinName),
		AskExchange:   toProtoExchange(msg.AskExchange),
		BidExchange:   toProtoExchange(msg.BidExchange),
		AskPrice:      msg.AskPrice,
		BidPrice:      msg.BidPrice,
		ProfitAbs:     msg.ProfitAbs,
		ProfitPercent: msg.ProfitPercent,
		Timestamp:     timestamppb.New(msg.Timestamp),
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
