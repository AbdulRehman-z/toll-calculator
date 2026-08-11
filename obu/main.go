package main

import (
	"context"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"
	"toll-calculator/shared"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/lmittmann/tint"
)

const wsURL = "ws://localhost:3000/ws"

func genCord() float64 {
	return float64(rand.IntN(100))
}

func getlatlng() (float64, float64) {
	return genCord(), genCord()
}

func genOBUIds(count int) []int {
	obuIds := make([]int, count)
	for i := range count {
		obuIds[i] = rand.IntN(math.MaxInt)
	}

	return obuIds
}

func connectAndStream(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	conn, _, err := websocket.Dial(dialCtx, wsURL, nil)
	if err != nil {
		return err
	}
	defer conn.CloseNow()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	slog.Info("Starting OBU simulation. Press Ctrl+C to stop.")

	obuIds := genOBUIds(1)
	for {
		select {
		case <-ticker.C:
			for i := range obuIds {
				lat, lng := getlatlng()
				data := shared.OBUData{
					OBUId: obuIds[i],
					Lat:   lat,
					Lng:   lng,
				}
				if err := wsjson.Write(context.Background(), conn, &data); err != nil {
					return err
				}

				slog.Info("sent data", "obu_id", obuIds[i], "lat", lat, "lng", lng)
			}
		case <-ctx.Done():
			slog.Info("Received signal, shutting down gracefully")
			conn.Close(websocket.StatusNormalClosure, "closing connection,bye")
			return ctx.Err()
		}
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	logger := slog.New(tint.NewTextHandler(os.Stdout, &tint.Options{
		Level: slog.LevelDebug,
	}))

	slog.SetDefault(logger)

	backOff := time.Second
	const maxBackOff = time.Second * 30

	for {
		connectedAt := time.Now()
		err := connectAndStream(ctx)

		if ctx.Err() != nil {
			return
		}

		if time.Since(connectedAt) > time.Second*10 {
			backOff = time.Second
		}

		slog.Error("connection lost, retrying...", "err", err, "retrying_in", backOff)

		select {
		case <-time.After(backOff):
		case <-ctx.Done():
			slog.Warn("shutting down during backoff")
			return
		}

		backOff = backOff * 2
		if backOff > maxBackOff {
			backOff = maxBackOff
		}
	}
}
