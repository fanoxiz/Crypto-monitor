package api

import (
	"context"
	"fmt"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/gorilla/websocket"
)

func stream(ctx context.Context, endpoint string, request any, heartbeat string, decode func([]byte) (contracts.Quote, error), save func(contracts.Quote) error) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		return err
	}

	if err := conn.WriteJSON(request); err != nil {
		return err
	}

	quoteDeadline := time.Now().Add(500 * time.Millisecond)

	if err := conn.SetReadDeadline(quoteDeadline); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})

	defer func() {
		cancel()
		conn.Close()
		<-heartbeatDone
	}()

	go func() {
		defer close(heartbeatDone)

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

		receivedAt := time.Now().UTC()

		if receivedAt.After(quoteDeadline) {
			return fmt.Errorf("no quote received within 500ms")
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

		quote.ReceivedAt = receivedAt

		quoteDeadline = receivedAt.Add(500 * time.Millisecond)

		if err := conn.SetReadDeadline(quoteDeadline); err != nil {
			return err
		}

		if err := save(quote); err != nil {
			return err
		}
	}
}
