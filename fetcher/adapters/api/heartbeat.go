package api

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

const heartbeatInterval = 20 * time.Second
const heartbeatTimeout = 5 * time.Second
const heartbeatPayload = "heartbeat"

func runHeartbeat(ctx context.Context, conn *websocket.Conn, logger *slog.Logger, packet string, received <-chan time.Time, failed chan<- error, onHealthy func()) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	fail := func(err error) {
		logger.Warn("heartbeat_failed", "error", err)
		failed <- err
	}

	for ctx.Err() == nil {
		select {
		case <-received:
		default:
		}

		sentAt := time.Now()
		var err error

		if packet == "" {
			err = conn.WriteControl(websocket.PingMessage, []byte(heartbeatPayload), sentAt.Add(heartbeatTimeout))
		} else {
			err = conn.SetWriteDeadline(sentAt.Add(heartbeatTimeout))
			if err == nil {
				err = conn.WriteMessage(websocket.TextMessage, []byte(packet))
			}
		}

		if err != nil {
			if ctx.Err() == nil {
				fail(fmt.Errorf("heartbeat write: %w", err))
			}

			return
		}

		logger.Debug("heartbeat_sent", "sent_at", sentAt.UTC(), "response_timeout", heartbeatTimeout)

		if err := waitHeartbeat(ctx, received, sentAt); err != nil {
			if ctx.Err() == nil {
				fail(err)
			}

			return
		}

		if ctx.Err() != nil {
			return
		}

		onHealthy()

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func waitHeartbeat(ctx context.Context, received <-chan time.Time, sentAt time.Time) error {
	deadline := time.Now().Add(heartbeatTimeout)
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case at := <-received:
			if at.Before(sentAt) {
				continue
			}

			if !at.After(deadline) {
				return nil
			}
		case <-timer.C:
			select {
			case at := <-received:
				if !at.Before(sentAt) && !at.After(deadline) {
					return nil
				}
			default:
			}
		}

		return fmt.Errorf("heartbeat response not received within %s", heartbeatTimeout)
	}
}
