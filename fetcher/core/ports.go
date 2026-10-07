package core

import "context"

type Exchange interface {
	Name() string
	Stream(ctx context.Context, coins []string, save func(Quote) error) error
}

type QuoteStore interface {
	Save(ctx context.Context, quote Quote) error
}
