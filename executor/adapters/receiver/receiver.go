package receiver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"github.com/fanoxiz/crypto-monitor/executor/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type GRPCReceiver struct {
	grpcpb.UnimplementedExecutorServiceServer
	executor core.Executor
}

func NewGRPCReceiver(executor core.Executor) *GRPCReceiver {
	return &GRPCReceiver{
		executor: executor,
	}
}

func (r *GRPCReceiver) Start(ctx context.Context, port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	defer listener.Close()

	server := grpc.NewServer()
	grpcpb.RegisterExecutorServiceServer(server, r)
	reflection.Register(server)

	stop := context.AfterFunc(ctx, server.GracefulStop)
	defer stop()

	slog.Info("server_started", "component", "receiver", "port", port)

	err = server.Serve(listener)

	slog.Info("server_stopped", "component", "receiver", "error", err)

	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}

	return err
}

func (r *GRPCReceiver) ProcessDeal(ctx context.Context, msg *grpcpb.ProfitDealInfo) (*emptypb.Empty, error) {
	startedAt := time.Now()

	if msg == nil || msg.GetDetectedAt() == nil {
		slog.Warn("deal_rejected", "component", "receiver", "reason", "missing_detection_time")

		return nil, status.Error(codes.InvalidArgument, "detection time is required")
	}

	if err := msg.GetDetectedAt().CheckValid(); err != nil {
		slog.Warn("deal_rejected", "component", "receiver", "reason", "invalid_detection_time", "error", err)

		return nil, status.Error(codes.InvalidArgument, "invalid detection time")
	}

	deal := contracts.ProfitDealInfo{
		Coin:           msg.GetCoin(),
		BuyExchange:    msg.GetBuyExchange(),
		SellExchange:   msg.GetSellExchange(),
		BuyAmountUSDT:  msg.GetBuyAmountUsdt(),
		SellAmountUSDT: msg.GetSellAmountUsdt(),
		ProfitUSDT:     msg.GetProfitUsdt(),
		DetectedAt:     msg.GetDetectedAt().AsTime(),
	}

	slog.Debug("deal_received", "component", "receiver", "coin", deal.Coin, "buy_exchange", deal.BuyExchange, "sell_exchange", deal.SellExchange, "buy_usdt", deal.BuyAmountUSDT, "sell_usdt", deal.SellAmountUSDT, "profit_usdt", deal.ProfitUSDT, "detected_at", deal.DetectedAt)

	if err := r.executor.ProcessDeal(ctx, deal); err != nil {
		if errors.Is(err, core.ErrInvalidDeal) {
			slog.Warn("deal_rejected", "component", "receiver", "coin", deal.Coin, "reason", "invalid_deal", "error", err)

			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		if ctx.Err() != nil {
			slog.Debug("deal_processing_cancelled", "component", "receiver", "coin", deal.Coin, "duration", time.Since(startedAt), "error", ctx.Err())

			return nil, status.FromContextError(ctx.Err()).Err()
		}

		slog.Error("deal_save_failed", "component", "receiver", "coin", deal.Coin, "duration", time.Since(startedAt), "error", err)

		return nil, status.Error(codes.Internal, "failed to save deal")
	}

	slog.Debug("deal_processed", "component", "receiver", "coin", deal.Coin, "duration", time.Since(startedAt))

	return &emptypb.Empty{}, nil
}
