package sender

import (
	"context"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GRPCSender struct {
	client grpcpb.ExecutorServiceClient
}

func NewGRPCSender(conn *grpc.ClientConn) *GRPCSender {
	return &GRPCSender{
		client: grpcpb.NewExecutorServiceClient(conn),
	}
}

func (s *GRPCSender) Send(ctx context.Context, deal contracts.ProfitDealInfo) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err := s.client.ProcessDeal(ctx, &grpcpb.ProfitDealInfo{
		Coin:           deal.Coin,
		BuyExchange:    deal.BuyExchange,
		SellExchange:   deal.SellExchange,
		BuyAmountUsdt:  deal.BuyAmountUSDT,
		SellAmountUsdt: deal.SellAmountUSDT,
		ProfitUsdt:     deal.ProfitUSDT,
		DetectedAt:     timestamppb.New(deal.DetectedAt),
	})

	return err
}
