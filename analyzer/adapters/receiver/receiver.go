package receiver

import (
	"context"
	"log"
	"net"

	"github.com/fanoxiz/crypto-monitor/analyzer/core"
	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"google.golang.org/grpc"
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
		CoinName:     rec.fromProtoCoin(msg.GetCoinName()),
		ExchangeName: rec.fromProtoExchange(msg.GetExchangeName()),
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

	log.Printf("level=INFO component=receiver event=server_started port=%s", port)
	return srv.Serve(lis)
}

func (rec *GRPCReceiver) fromProtoCoin(coin grpcpb.CoinName) string {
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

func (rec *GRPCReceiver) fromProtoExchange(exchange grpcpb.ExchangeName) string {
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
