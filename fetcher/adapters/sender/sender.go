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

	_, err := s.client.ProcessPrices(ctx, &grpcpb.MarketTickerInfo{
		CoinName:     contracts.ToProtoCoin(msg.CoinName),
		ExchangeName: contracts.ToProtoExchange(msg.ExchangeName),
		Price: &grpcpb.BidAsk{
			Bid: msg.Price.Bid,
			Ask: msg.Price.Ask,
		},
	})
	if err != nil {
		return fmt.Errorf("send to analyzer: %w", err)
	}
	return nil
}
