package api

import (
	"context"
	"time"

	"github.com/fanoxiz/crypto-monitor/fetcher/core"
	"github.com/gorilla/websocket"
)

func stream(ctx context.Context, endpoint string, request any, heartbeat string, decode func([]byte) (core.Quote, error), save func(core.Quote) error) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.WriteJSON(request); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				conn.Close()
				return
			case <-ticker.C:
				if heartbeat == "" {
					continue
				}

				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

				if err := conn.WriteMessage(websocket.TextMessage, []byte(heartbeat)); err != nil {
					conn.Close()
					return
				}
			}
		}
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		if string(data) == "pong" {
			continue
		}

		quote, err := decode(data)
		if err != nil {
			return err
		}

		if quote.Coin == "" {
			continue
		}

		quote.ReceivedAt = time.Now().UTC()

		if err := save(quote); err != nil {
			return err
		}
	}
}
