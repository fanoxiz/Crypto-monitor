package sender

import (
	"context"
	"log/slog"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
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

	startedAt := time.Now()

	slog.Debug("executor_rpc_starting", "component", "sender", "coin", deal.Coin, "buy_exchange", deal.BuyExchange, "sell_exchange", deal.SellExchange, "timeout", 3*time.Second)

	_, err := s.client.ProcessDeal(ctx, &grpcpb.ProfitDealInfo{
		Coin:           deal.Coin,
		BuyExchange:    deal.BuyExchange,
		SellExchange:   deal.SellExchange,
		BuyAmountUsdt:  deal.BuyAmountUSDT,
		SellAmountUsdt: deal.SellAmountUSDT,
		ProfitUsdt:     deal.ProfitUSDT,
		DetectedAt:     timestamppb.New(deal.DetectedAt),
	})

	slog.Debug("executor_rpc_finished", "component", "sender", "coin", deal.Coin, "duration", time.Since(startedAt), "grpc_code", status.Code(err).String(), "error", err)

	return err
}
