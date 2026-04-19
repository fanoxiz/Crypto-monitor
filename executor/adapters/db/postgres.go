package db

import (
	"context"
	"fmt"

	_ "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type PostgresRepo struct {
	pool           *pgxpool.Pool
	initialBalance float64
}

func NewPostgresRepo(ctx context.Context, connString string, initialBalance float64) (*PostgresRepo, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to db: %w", err)
	}

	repo := &PostgresRepo{
		pool:           pool,
		initialBalance: initialBalance,
	}
	if err := repo.initSchema(ctx); err != nil {
		return nil, err
	}
	return repo, nil
}

func (r *PostgresRepo) initSchema(ctx context.Context) error {
	dealsQuery := `
		CREATE TABLE IF NOT EXISTS deals (
			id SERIAL PRIMARY KEY,
			coin VARCHAR(20),
			buy_exchange VARCHAR(50),
			sell_exchange VARCHAR(50),
			profit_percent NUMERIC(10, 4),
			earned_usd NUMERIC(15, 4),
			created_at TIMESTAMP
		);`
	if _, err := r.pool.Exec(ctx, dealsQuery); err != nil {
		return err
	}

	accountQuery := `
		CREATE TABLE IF NOT EXISTS account (
			id INT PRIMARY KEY,
			balance NUMERIC(15, 4)
		);`
	if _, err := r.pool.Exec(ctx, accountQuery); err != nil {
		return err
	}

	_, err := r.pool.Exec(ctx, "INSERT INTO account (id, balance) VALUES (1, $1) ON CONFLICT DO NOTHING", r.initialBalance)
	return err
}

func (r *PostgresRepo) SaveDealAndUpdateBalance(ctx context.Context, deal contracts.ProfitDealInfo, earnedUSD float64) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		"INSERT INTO deals (coin, buy_exchange, sell_exchange, profit_percent, earned_usd, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
		deal.CoinName, deal.AskExchange, deal.BidExchange, deal.ProfitPercent, earnedUSD, deal.Timestamp,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, "UPDATE account SET balance = balance + $1 WHERE id = 1", earnedUSD)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *PostgresRepo) GetBalance(ctx context.Context) (float64, error) {
	var balance float64
	err := r.pool.QueryRow(ctx, "SELECT balance FROM account WHERE id = 1").Scan(&balance)
	return balance, err
}

func (r *PostgresRepo) GetDealsCount(ctx context.Context) (int64, error) {
	var count int64
	err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM deals").Scan(&count)
	return count, err
}
