package receiver

import (
	"context"
	"log"
	"net"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"github.com/fanoxiz/crypto-monitor/executor/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GRPCReceiver struct {
	grpcpb.UnimplementedExecutorServiceServer
	executor core.Executor
}

func NewGRPCReceiver(ex core.Executor) *GRPCReceiver {
	return &GRPCReceiver{executor: ex}
}

func (rec *GRPCReceiver) Start(port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	srv := grpc.NewServer()
	grpcpb.RegisterExecutorServiceServer(srv, rec)
	reflection.Register(srv)

	log.Printf("level=INFO component=receiver event=server_started port=%s", port)
	return srv.Serve(lis)
}

func (rec *GRPCReceiver) ProcessDeal(ctx context.Context, msg *grpcpb.ProfitDealInfo) (*emptypb.Empty, error) {
	deal := contracts.ProfitDealInfo{
		CoinName:      contracts.FromProtoCoin(msg.GetCoinName()),
		AskExchange:   contracts.FromProtoExchange(msg.GetAskExchange()),
		BidExchange:   contracts.FromProtoExchange(msg.GetBidExchange()),
		AskPrice:      msg.GetAskPrice(),
		BidPrice:      msg.GetBidPrice(),
		ProfitAbs:     msg.GetProfitAbs(),
		ProfitPercent: msg.GetProfitPercent(),
		Timestamp:     msg.GetTimestamp().AsTime(),
	}

	if err := rec.executor.ProcessDeal(ctx, deal); err != nil {
		log.Printf("level=ERROR component=receiver event=process_deal_failed err=\"%v\"", err)
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (rec *GRPCReceiver) GetStats(ctx context.Context, _ *emptypb.Empty) (*grpcpb.DealStats, error) {
	stats, err := rec.executor.GetStats(ctx)
	if err != nil {
		log.Printf("level=ERROR component=receiver event=stats_failed err=\"%v\"", err)
		return nil, err
	}

	recent := make([]*grpcpb.RecentDeal, len(stats.RecentDeals))
	for i, d := range stats.RecentDeals {
		recent[i] = &grpcpb.RecentDeal{
			Coin:          d.Coin,
			BuyExchange:   d.BuyExchange,
			SellExchange:  d.SellExchange,
			ProfitPercent: d.ProfitPercent,
			EarnedUsd:     d.EarnedUSD,
			CreatedAt:     timestamppb.New(d.CreatedAt),
		}
	}

	return &grpcpb.DealStats{
		CurrentBalance: stats.CurrentBalance,
		TotalEarned:    stats.TotalEarned,
		DealsCount:     stats.DealsCount,
		RecentDeals:    recent,
	}, nil
}
