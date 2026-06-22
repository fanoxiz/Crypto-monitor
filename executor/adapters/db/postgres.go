package db

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/executor/core"
)

type PostgresRepo struct {
	pool           *pgxpool.Pool
	initialBalance float64
}

func NewPostgresRepo(ctx context.Context, connString string, initialBalance float64) (*PostgresRepo, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
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
		return fmt.Errorf("postgres init schema: create deals table: %w", err)
	}

	accountQuery := `
		CREATE TABLE IF NOT EXISTS account (
			id INT PRIMARY KEY,
			balance NUMERIC(15, 4)
		);`
	if _, err := r.pool.Exec(ctx, accountQuery); err != nil {
		return fmt.Errorf("postgres init schema: create account table: %w", err)
	}

	_, err := r.pool.Exec(ctx, "INSERT INTO account (id, balance) VALUES (1, $1) ON CONFLICT DO NOTHING", r.initialBalance)
	if err != nil {
		return fmt.Errorf("postgres init schema: seed account: %w", err)
	}

	return nil
}

func (r *PostgresRepo) SaveDealAndUpdateBalance(ctx context.Context, deal contracts.ProfitDealInfo, earnedUSD float64) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		"INSERT INTO deals (coin, buy_exchange, sell_exchange, profit_percent, earned_usd, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
		deal.CoinName, deal.AskExchange, deal.BidExchange, deal.ProfitPercent, earnedUSD, deal.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("postgres insert deal: %w", err)
	}
	log.Printf(
		"level=INFO component=db event=deal_saved coin=%s buy_exchange=%s sell_exchange=%s ask=%.6f bid=%.6f earned=%.6f profit_percent=%.4f",
		deal.CoinName,
		deal.AskExchange,
		deal.BidExchange,
		deal.AskPrice,
		deal.BidPrice,
		earnedUSD,
		deal.ProfitPercent,
	)
	_, err = tx.Exec(ctx, "UPDATE account SET balance = balance + $1 WHERE id = 1", earnedUSD)
	if err != nil {
		return fmt.Errorf("postgres update balance: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres commit tx: %w", err)
	}

	return nil
}

func (r *PostgresRepo) GetBalance(ctx context.Context) (float64, error) {
	var balance float64
	if err := r.pool.QueryRow(ctx, "SELECT balance FROM account WHERE id = 1").Scan(&balance); err != nil {
		return 0, fmt.Errorf("postgres get balance: %w", err)
	}
	return balance, nil
}

func (r *PostgresRepo) GetDealsCount(ctx context.Context) (int64, error) {
	var count int64
	if err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM deals").Scan(&count); err != nil {
		return 0, fmt.Errorf("postgres get deals count: %w", err)
	}
	return count, nil
}

func (r *PostgresRepo) GetRecentDeals(ctx context.Context, limit int) ([]core.RecentDeal, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT coin, buy_exchange, sell_exchange, profit_percent, earned_usd, created_at
		FROM deals
		ORDER BY created_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres get recent deals: %w", err)
	}
	defer rows.Close()

	deals := make([]core.RecentDeal, 0, limit)
	for rows.Next() {
		var d core.RecentDeal
		if err := rows.Scan(&d.Coin, &d.BuyExchange, &d.SellExchange, &d.ProfitPercent, &d.EarnedUSD, &d.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres scan recent deal: %w", err)
		}
		deals = append(deals, d)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres iterate recent deals: %w", err)
	}

	return deals, nil
}
