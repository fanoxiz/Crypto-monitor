package api

import (
	"encoding/json"
	"time"
)

func sourceTime(timestamp json.Number) time.Time {
	milliseconds, err := timestamp.Int64()
	if err != nil || milliseconds <= 0 {
		return time.Time{}
	}

	return time.UnixMilli(milliseconds).UTC()
}
