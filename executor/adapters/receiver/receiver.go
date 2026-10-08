package receiver

import (
	"context"
	"errors"
	"log"
	"net"

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

	log.Printf("level=INFO component=receiver event=server_started port=%s", port)

	err = server.Serve(listener)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}

	return err
}

func (r *GRPCReceiver) ProcessDeal(ctx context.Context, msg *grpcpb.ProfitDealInfo) (*emptypb.Empty, error) {
	if msg == nil || msg.GetDetectedAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "detection time is required")
	}

	if err := msg.GetDetectedAt().CheckValid(); err != nil {
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

	if err := r.executor.ProcessDeal(ctx, deal); err != nil {
		if errors.Is(err, core.ErrInvalidDeal) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}

		log.Printf("level=ERROR component=receiver event=deal_save_failed err=%q", err)

		return nil, status.Error(codes.Internal, "failed to save deal")
	}

	return &emptypb.Empty{}, nil
}
