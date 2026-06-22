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

	_, err := s.client.ProcessDeal(ctx, &grpcpb.ProfitDealInfo{
		CoinName:      contracts.ToProtoCoin(msg.CoinName),
		AskExchange:   contracts.ToProtoExchange(msg.AskExchange),
		BidExchange:   contracts.ToProtoExchange(msg.BidExchange),
		AskPrice:      msg.AskPrice,
		BidPrice:      msg.BidPrice,
		ProfitAbs:     msg.ProfitAbs,
		ProfitPercent: msg.ProfitPercent,
		Timestamp:     timestamppb.New(msg.Timestamp),
	})
	if err != nil {
		return fmt.Errorf("send to executor: %w", err)
	}
	return nil
}
