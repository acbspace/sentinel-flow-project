// Package httpx holds the HTTP server plumbing shared by every SentinelFlow
// service: timeout-hardened servers, graceful shutdown, health probes and the
// metrics/access-log middleware.
package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// ServerOptions tunes the timeouts applied to a service's HTTP server.
type ServerOptions struct {
	Addr              string
	Handler           http.Handler
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

// NewServer builds an http.Server with every timeout set.
//
// Go's zero-value server has no timeouts at all, which lets a slow or malicious
// client hold a connection open indefinitely. These defaults are deliberately
// conservative for a JSON API that does no streaming.
func NewServer(opts ServerOptions) *http.Server {
	if opts.ReadHeaderTimeout <= 0 {
		opts.ReadHeaderTimeout = 5 * time.Second
	}
	if opts.ReadTimeout <= 0 {
		opts.ReadTimeout = 15 * time.Second
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 30 * time.Second
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = 60 * time.Second
	}

	return &http.Server{
		Addr:              opts.Addr,
		Handler:           opts.Handler,
		ReadHeaderTimeout: opts.ReadHeaderTimeout,
		ReadTimeout:       opts.ReadTimeout,
		WriteTimeout:      opts.WriteTimeout,
		IdleTimeout:       opts.IdleTimeout,
	}
}

// Serve runs srv until ctx is cancelled, then drains in-flight requests.
//
// It returns nil on a clean shutdown. If the drain exceeds grace, the remaining
// connections are closed and the timeout error is returned so the caller can
// exit non-zero and make the rough shutdown visible.
func Serve(ctx context.Context, srv *http.Server, grace time.Duration, log *slog.Logger) error {
	errCh := make(chan error, 1)

	go func() {
		log.Info("http server listening", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Info("http server shutting down", slog.Duration("grace", grace))

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), grace)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Shutdown returned before every connection drained; force them closed
		// so the process can exit instead of hanging.
		_ = srv.Close()
		return err
	}

	// ListenAndServe returns ErrServerClosed (mapped to nil) once Shutdown wins.
	return <-errCh
}
