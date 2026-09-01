package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/tractoai/ytsaurus-identity-sync/internal/app"
	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	"go.uber.org/zap"
)

const serverShutdownTimeout = 5 * time.Second

type Server struct {
	syncer     Syncer
	logger     *zap.SugaredLogger
	httpServer *http.Server

	stopOnce sync.Once
}

// Syncer starts application sync iterations.
type Syncer interface {
	SyncAsync() error
}

func New(logger *zap.SugaredLogger, cfg *config.ServerConfig, application Syncer) *Server {
	listen := cfg.Listen
	if listen == "" {
		listen = config.DefaultServerListen
	}

	server := &Server{
		syncer: application,
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", server.health)
	mux.HandleFunc("/readyz", server.health)
	mux.HandleFunc("/sync", server.sync)
	server.httpServer = &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return server
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.httpServer.Addr, err)
	}
	s.logger.Infow("Starting health server", "listen", listener.Addr().String())

	go func() {
		if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Errorw("Health server failed", zap.Error(err))
		}
	}()

	return nil
}

func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
		defer cancel()
		if err := s.httpServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Errorw("Failed to stop health server", zap.Error(err))
		}
	})
}

func (s *Server) health(response http.ResponseWriter, _ *http.Request) {
	response.WriteHeader(http.StatusOK)
}

func (s *Server) sync(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		http.Error(response, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	if err := s.syncer.SyncAsync(); err != nil {
		if errors.Is(err, app.ErrSyncInProgress) {
			http.Error(response, err.Error(), http.StatusConflict)
			return
		}

		s.logger.Errorw("Failed to start sync", zap.Error(err))
		http.Error(response, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	response.WriteHeader(http.StatusAccepted)
}
