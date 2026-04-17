package core

import "github.com/fanoxiz/crypto-monitor/contracts"

type Executor interface {
	ProcessDeal(msg contracts.ProfitDealInfo) error
}
