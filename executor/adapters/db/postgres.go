package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(ctx context.Context, connString string) (*PostgresRepo, error) {
	slog.Info("postgres_connecting", "component", "db")

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS profit_deals (
			id BIGSERIAL PRIMARY KEY,
			saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			payload JSONB NOT NULL
		)
	`)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres init schema: %w", err)
	}

	slog.Info("postgres_schema_ready", "component", "db", "table", "profit_deals")

	return &PostgresRepo{
		pool: pool,
	}, nil
}

func (r *PostgresRepo) Close() {
	r.pool.Close()

	slog.Info("postgres_closed", "component", "db")
}

func (r *PostgresRepo) SaveDeal(ctx context.Context, deal contracts.ProfitDealInfo) error {
	startedAt := time.Now()

	payload, err := json.Marshal(deal)
	if err != nil {
		slog.Debug("deal_payload_failed", "component", "db", "coin", deal.Coin, "error", err)

		return err
	}

	slog.Debug("deal_insert_starting", "component", "db", "coin", deal.Coin, "payload_bytes", len(payload))

	_, err = r.pool.Exec(ctx, "INSERT INTO profit_deals (payload) VALUES ($1::jsonb)", string(payload))
	if err != nil {
		slog.Debug("deal_insert_failed", "component", "db", "coin", deal.Coin, "duration", time.Since(startedAt), "error", err)

		return fmt.Errorf("postgres save deal: %w", err)
	}

	slog.Info("deal_saved", "component", "db", "coin", deal.Coin, "buy_exchange", deal.BuyExchange, "sell_exchange", deal.SellExchange, "buy_usdt", deal.BuyAmountUSDT, "sell_usdt", deal.SellAmountUSDT, "profit_usdt", deal.ProfitUSDT, "duration", time.Since(startedAt))

	return nil
}
