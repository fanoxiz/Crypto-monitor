package core

import "time"

type marketQuote struct {
	exchange     string
	askPrice     float64
	bidPrice     float64
	askSize      float64
	bidSize      float64
	buyCost      float64
	sellProceeds float64
	receivedAt   time.Time
	expiresAt    time.Time
}

type dealFingerprint struct {
	buyExchange  string
	sellExchange string
	buyPrice     float64
	sellPrice    float64
	quantity     float64
}
