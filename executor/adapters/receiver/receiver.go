package receiver

import (
	"context"
	"log"
	"net"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"github.com/fanoxiz/crypto-monitor/executor/core"
	"google.golang.org/grpc"
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

	log.Printf("level=INFO component=receiver event=server_started port=%s", port)
	return srv.Serve(lis)
}

func (rec *GRPCReceiver) ProcessDeal(ctx context.Context, msg *grpcpb.ProfitDealInfo) (*emptypb.Empty, error) {
	deal := contracts.ProfitDealInfo{
		CoinName:      fromProtoCoin(msg.GetCoinName()),
		AskExchange:   fromProtoExchange(msg.GetAskExchange()),
		BidExchange:   fromProtoExchange(msg.GetBidExchange()),
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

func fromProtoCoin(coin grpcpb.CoinName) string {
	switch coin {
	case grpcpb.CoinName_COIN_BTC:
		return "BTC"
	case grpcpb.CoinName_COIN_ETH:
		return "ETH"
	case grpcpb.CoinName_COIN_XAUT:
		return "XAUt"
	default:
		return ""
	}
}

func fromProtoExchange(exchange grpcpb.ExchangeName) string {
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
