package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
	"toll-calculator/shared"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/lmittmann/tint"
)

var KafkaTopic = "obu-data"

type DataRevceiver struct {
	addr     string
	dataCh   chan shared.OBUData
	producer DataProducer
	connWG   sync.WaitGroup

	mu    sync.RWMutex
	conns map[*websocket.Conn]struct{}
}

func NewDataReceiver(addr string) (*DataRevceiver, error) {
	p, err := NewKafkaProducer()
	if err != nil {
		return nil, err
	}

	return &DataRevceiver{
		addr:     addr,
		dataCh:   make(chan shared.OBUData, 1028),
		producer: p,
		conns:    map[*websocket.Conn]struct{}{},
	}, nil
}

func main() {
	logger := slog.New(tint.NewTextHandler(os.Stdout, &tint.Options{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	dr, err := NewDataReceiver(":3000")
	if err != nil {
		slog.Error("failed to create data receiver", "err", err.Error())
		return
	}

	var workerWG sync.WaitGroup
	workers := runtime.NumCPU()
	for i := range workers {
		workerWG.Add(1)
		go func(i int) {
			defer workerWG.Done()
			for data := range dr.dataCh {
				if err := dr.producer.ProduceData(data); err != nil {
					slog.Error("failed to produce data", "err", err.Error())
				}
			}
		}(i)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", dr.WsHandler)
	srv := &http.Server{
		Addr:    dr.addr,
		Handler: mux,
	}

	go func() {
		slog.Info("server starting", "addr", dr.addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err.Error())
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown signal received")

	// stop accepting new connections
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second*2)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("failed graceful shutdown of server", "err", err)
	}

	// close all the opened connections
	dr.closeAllConns()
	// wait for all connection handlers to actually exit
	dr.connWG.Wait()
	// now safe to close BECAUSE guaranteed no one is sending anymore
	close(dr.dataCh)
	// wait for all the in-flight messages to be sent
	dr.producer.Flush(5000)
	// close the producer
	dr.producer.Close()
	// let workers drain whatever's left, then exit
	workerWG.Wait()

	slog.Info("shutdown complete")
}

func (dr *DataRevceiver) closeAllConns() {
	dr.mu.Lock()
	defer dr.mu.Unlock()
	for conn := range dr.conns {
		conn.Close(websocket.StatusServiceRestart, "caio")
	}
}

func (dr *DataRevceiver) WsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		slog.Error("error accepting websocket conn", "err", err)
		return
	}

	dr.mu.Lock()
	dr.conns[conn] = struct{}{}
	dr.mu.Unlock()

	dr.connWG.Add(1)
	go dr.handleWsConn(conn)
}

func (dr *DataRevceiver) handleWsConn(conn *websocket.Conn) {
	defer func() {
		dr.connWG.Done()
		conn.CloseNow()

		dr.mu.Lock()
		delete(dr.conns, conn)
		dr.mu.Unlock()
	}()

	for {
		var data shared.OBUData
		if err := wsjson.Read(context.Background(), conn, &data); err != nil {
			slog.Error("error reading from ws conn", "err", err.Error())
			return
		}

		select {
		case dr.dataCh <- data:
		default:
			slog.Warn("dataCh full, dropping obu", "obu", data)
		}
	}
}
