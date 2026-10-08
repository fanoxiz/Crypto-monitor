package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(ctx context.Context, connString string) (*PostgresRepo, error) {
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

	return &PostgresRepo{
		pool: pool,
	}, nil
}

func (r *PostgresRepo) Close() {
	r.pool.Close()
}

func (r *PostgresRepo) SaveDeal(ctx context.Context, deal contracts.ProfitDealInfo) error {
	payload, err := json.Marshal(deal)
	if err != nil {
		return err
	}

	_, err = r.pool.Exec(ctx, "INSERT INTO profit_deals (payload) VALUES ($1::jsonb)", string(payload))
	if err != nil {
		return fmt.Errorf("postgres save deal: %w", err)
	}

	log.Printf("level=INFO component=db event=deal_saved coin=%s buy_exchange=%s sell_exchange=%s buy_usdt=%.8g sell_usdt=%.8g profit_usdt=%.8g",
		deal.Coin,
		deal.BuyExchange,
		deal.SellExchange,
		deal.BuyAmountUSDT,
		deal.SellAmountUSDT,
		deal.ProfitUSDT,
	)

	return nil
}
