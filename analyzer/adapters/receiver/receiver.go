package receiver

import (
	"context"
	"log"
	"net"

	"github.com/fanoxiz/crypto-monitor/analyzer/core"
	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
)

type GRPCReceiver struct {
	grpcpb.UnimplementedAnalyzerServiceServer
	analyzer core.Analyzer
}

func NewGRPCReceiver(an core.Analyzer) *GRPCReceiver {
	return &GRPCReceiver{
		analyzer: an,
	}
}

func (rec *GRPCReceiver) ProcessPrices(ctx context.Context, msg *grpcpb.MarketTickerInfo) (*emptypb.Empty, error) {
	domainMsg := contracts.MarketTickerInfo{
		CoinName:     contracts.FromProtoCoin(msg.GetCoinName()),
		ExchangeName: contracts.FromProtoExchange(msg.GetExchangeName()),
		Price: contracts.BidAsk{
			Bid: msg.GetPrice().GetBid(),
			Ask: msg.GetPrice().GetAsk(),
		},
	}

	if err := rec.analyzer.ProcessPrices(domainMsg); err != nil {
		log.Printf("level=ERROR component=receiver event=process_prices_failed err=\"%v\"", err)
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (rec *GRPCReceiver) Start(port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	srv := grpc.NewServer()
	grpcpb.RegisterAnalyzerServiceServer(srv, rec)
	reflection.Register(srv)

	log.Printf("level=INFO component=receiver event=server_started port=%s", port)
	return srv.Serve(lis)
}
