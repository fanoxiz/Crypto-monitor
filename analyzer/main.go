package main

import (
	"context"
	"log"

	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/cache"
	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/receiver"
	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/sender"
	"github.com/fanoxiz/crypto-monitor/analyzer/config"
	"github.com/fanoxiz/crypto-monitor/analyzer/core"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log.SetPrefix("service=analyzer ")

	cfg, err := config.Load("analyzer/config.yaml")
	if err != nil {
		log.Fatalf("level=ERROR component=main event=config_load_failed err=\"%v\"", err)
	}

	grpcConn, err := grpc.NewClient(cfg.ExecutorGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("level=ERROR component=main event=grpc_dial_failed err=\"%v\"", err)
	}
	defer grpcConn.Close()

	senderService := sender.NewSenderService(grpcConn)

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("level=ERROR component=main event=redis_connect_failed err=\"%v\"", err)
	}

	priceStore := cache.NewRedisPriceStore(redisClient, cfg.RedisKeyPrefix, cfg.PriceTTL)
	analyzerService := core.NewAnalyzerService(cfg.Fees, senderService, priceStore)
	grpcReceiver := receiver.NewGRPCReceiver(analyzerService)

	log.Printf("level=INFO component=main event=service_started port=%s", cfg.GRPCPort)
	if err := grpcReceiver.Start(cfg.GRPCPort); err != nil {
		log.Fatalf("level=ERROR component=main event=server_start_failed err=\"%v\"", err)
	}
}
