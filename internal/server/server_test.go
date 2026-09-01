package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tractoai/ytsaurus-identity-sync/internal/app"
	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	"github.com/tractoai/ytsaurus-identity-sync/internal/logging"
)

type fakeSyncer struct {
	err   error
	calls int
}

func (f *fakeSyncer) SyncAsync() error {
	f.calls++
	return f.err
}

func TestNew(t *testing.T) {
	application := &app.App{}
	server := New(logging.NewDevelopment(), &config.ServerConfig{}, application)

	require.Same(t, application, server.syncer)
	require.Equal(t, config.DefaultServerListen, server.httpServer.Addr)
}

func TestHealthEndpoints(t *testing.T) {
	server := New(
		logging.NewDevelopment(),
		&config.ServerConfig{Listen: "127.0.0.1:0"},
		&app.App{},
	)

	for _, path := range []string{"/livez", "/readyz"} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			response := httptest.NewRecorder()

			server.httpServer.Handler.ServeHTTP(response, request)

			require.Equal(t, http.StatusOK, response.Code)
		})
	}
}

func TestSync(t *testing.T) {
	testCases := []struct {
		name           string
		method         string
		syncErr        error
		expectedStatus int
		expectedCalls  int
	}{
		{
			name:           "accepted",
			method:         http.MethodPost,
			expectedStatus: http.StatusAccepted,
			expectedCalls:  1,
		},
		{
			name:           "sync-in-progress",
			method:         http.MethodPost,
			syncErr:        app.ErrSyncInProgress,
			expectedStatus: http.StatusConflict,
			expectedCalls:  1,
		},
		{
			name:           "unexpected-error",
			method:         http.MethodPost,
			syncErr:        errors.New("unexpected error"),
			expectedStatus: http.StatusInternalServerError,
			expectedCalls:  1,
		},
		{
			name:           "method-not-allowed",
			method:         http.MethodGet,
			expectedStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			application := &fakeSyncer{err: testCase.syncErr}
			server := New(
				logging.NewDevelopment(),
				&config.ServerConfig{},
				application,
			)
			request := httptest.NewRequest(testCase.method, "/sync", nil)
			response := httptest.NewRecorder()

			server.httpServer.Handler.ServeHTTP(response, request)

			require.Equal(t, testCase.expectedStatus, response.Code)
			require.Equal(t, testCase.expectedCalls, application.calls)
		})
	}
}
