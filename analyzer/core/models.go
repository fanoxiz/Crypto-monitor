package core

import "time"

type marketQuote struct {
	exchange     string
	askSize      float64
	bidSize      float64
	buyCost      float64
	sellProceeds float64
	receivedAt   time.Time
	expiresAt    time.Time
}
