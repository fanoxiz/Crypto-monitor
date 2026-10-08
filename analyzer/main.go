package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/cache"
	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/sender"
	"github.com/fanoxiz/crypto-monitor/analyzer/config"
	"github.com/fanoxiz/crypto-monitor/analyzer/core"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log.SetPrefix("service=analyzer ")
	log.SetFlags(log.LstdFlags | log.LUTC)

	if err := run(); err != nil {
		log.Fatalf("level=ERROR component=main event=startup_failed err=%q", err)
	}
}

func run() error {
	cfg, err := config.Load("analyzer/config.yaml")
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		return err
	}

	conn, err := grpc.NewClient(cfg.ExecutorGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	store := cache.NewRedisQuoteStore(client, cfg.TrackedCoins, cfg.Exchanges)
	dealSender := sender.NewGRPCSender(conn)
	service := core.NewAnalyzerService(cfg.Fees, store, dealSender, cfg.MaxQuoteAge)

	service.Run(ctx)

	return nil
}
