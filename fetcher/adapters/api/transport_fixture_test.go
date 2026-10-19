package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type heartbeatPipeListener struct {
	conn     net.Conn
	accepted bool
	closed   chan struct{}
	once     sync.Once
}

func (l *heartbeatPipeListener) Accept() (net.Conn, error) {
	if !l.accepted {
		l.accepted = true

		return l.conn, nil
	}

	<-l.closed

	return nil, net.ErrClosed
}

func (l *heartbeatPipeListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
	})

	return nil
}

func (l *heartbeatPipeListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

type heartbeatAcceptedSocket struct {
	conn *websocket.Conn
	err  error
}

func heartbeatTransport(t *testing.T) (*websocket.Dialer, <-chan heartbeatAcceptedSocket, func()) {
	t.Helper()

	client, peer := net.Pipe()
	listener := &heartbeatPipeListener{
		conn:   peer,
		closed: make(chan struct{}),
	}
	accepted := make(chan heartbeatAcceptedSocket, 1)
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrader := websocket.Upgrader{}
			conn, err := upgrader.Upgrade(w, r, nil)
			accepted <- heartbeatAcceptedSocket{
				conn: conn,
				err:  err,
			}
		}),
	}
	serverDone := make(chan error, 1)

	go func() {
		serverDone <- server.Serve(listener)
	}()

	var closeOnce sync.Once
	closeTransport := func() {
		closeOnce.Do(func() {
			client.Close()
			peer.Close()
			server.Close()

			if err := <-serverDone; !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("in-memory WebSocket server: %v", err)
			}
		})
	}
	dialer := &websocket.Dialer{
		HandshakeTimeout: time.Second,
		NetDialContext: func(context.Context, string, string) (net.Conn, error) {
			return client, nil
		},
	}

	return dialer, accepted, closeTransport
}

func heartbeatSockets(t *testing.T) (*websocket.Conn, *websocket.Conn, func()) {
	t.Helper()

	dialer, accepted, closeTransport := heartbeatTransport(t)
	client, _, err := dialer.Dial("ws://heartbeat.test/stream", nil)
	if err != nil {
		closeTransport()
		t.Fatal(err)
	}

	server := <-accepted
	if server.err != nil {
		closeTransport()
		t.Fatal(server.err)
	}

	return client, server.conn, closeTransport
}

type heartbeatPacket struct {
	kind int
	data string
	at   time.Time
}

func observeHeartbeatPackets(peer *websocket.Conn) (<-chan heartbeatPacket, <-chan struct{}) {
	packets := make(chan heartbeatPacket, 16)
	done := make(chan struct{})
	peer.SetPingHandler(func(payload string) error {
		packets <- heartbeatPacket{
			kind: websocket.PingMessage,
			data: payload,
			at:   time.Now(),
		}

		return nil
	})

	go func() {
		defer close(done)

		for {
			kind, data, err := peer.ReadMessage()
			if err != nil {
				return
			}

			packets <- heartbeatPacket{
				kind: kind,
				data: string(data),
				at:   time.Now(),
			}
		}
	}()

	return packets, done
}

func takeHeartbeatPacket(t *testing.T, packets <-chan heartbeatPacket) heartbeatPacket {
	t.Helper()

	select {
	case packet := <-packets:
		return packet
	default:
		t.Fatal("expected heartbeat packet was not observed after synchronization")

		return heartbeatPacket{}
	}
}
