// services/fetcher/core/ports.go

package core

type Exchange interface {
	GetName() string
	GetPrice(symbol string) (float64, error)
}

