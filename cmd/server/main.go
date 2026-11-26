// Command server runs the bulk document processing service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"bulkdoc/internal/httpapi"
	"bulkdoc/internal/principal"
	"bulkdoc/internal/repository"
	"bulkdoc/internal/repository/memory"
	"bulkdoc/internal/repository/postgres"
	"bulkdoc/internal/service"
)

type config struct {
	httpAddr         string
	databaseURL      string
	principalURL     string
	submitTimeout    time.Duration
	maxInFlight      int
	leaseDuration    time.Duration
	recoveryInterval time.Duration
	backoffBase      time.Duration
	backoffMax       time.Duration
	shutdownTimeout  time.Duration
	logLevel         slog.Level
}

func loadConfig(getenv func(string) string) (config, error) {
	cfg := config{
		httpAddr:         ":8080",
		databaseURL:      getenv("DATABASE_URL"),
		principalURL:     "http://localhost:9090",
		submitTimeout:    10 * time.Second,
		maxInFlight:      service.DefaultMaxInFlight,
		leaseDuration:    10 * time.Minute,
		recoveryInterval: 30 * time.Second,
		backoffMax:       5 * time.Minute,
		shutdownTimeout:  20 * time.Second,
	}
	if v := getenv("HTTP_ADDR"); v != "" {
		cfg.httpAddr = v
	}
	if v := getenv("PRINCIPAL_URL"); v != "" {
		cfg.principalURL = v
	}

	var errs []error
	duration := func(key string, dst *time.Duration) {
		if v := getenv(key); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 {
				errs = append(errs, fmt.Errorf("%s: invalid duration %q", key, v))
				return
			}
			*dst = d
		}
	}
	duration("SUBMIT_TIMEOUT", &cfg.submitTimeout)
	duration("LEASE_DURATION", &cfg.leaseDuration)
	duration("RECOVERY_INTERVAL", &cfg.recoveryInterval)
	duration("BACKOFF_BASE", &cfg.backoffBase)
	duration("BACKOFF_MAX", &cfg.backoffMax)
	duration("SHUTDOWN_TIMEOUT", &cfg.shutdownTimeout)

	if v := getenv("MAX_IN_FLIGHT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			errs = append(errs, fmt.Errorf("MAX_IN_FLIGHT: must be a positive integer, got %q", v))
		} else {
			cfg.maxInFlight = n
		}
	}
	if v := getenv("LOG_LEVEL"); v != "" {
		if err := cfg.logLevel.UnmarshalText([]byte(v)); err != nil {
			errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
		}
	}
	return cfg, errors.Join(errs...)
}

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe /healthz on the local HTTP address and exit")
	flag.Parse()

	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(2)
	}
	if *healthcheck {
		if err := probe(cfg.httpAddr); err != nil {
			fmt.Fprintln(os.Stderr, "unhealthy:", err)
			os.Exit(1)
		}
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.logLevel}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("server stopped with error", "error", err)
		os.Exit(1)
	}
}

// probe is the container health check (distroless images have no curl).
func probe(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func run(ctx context.Context, cfg config, logger *slog.Logger) error {
	repo, closeRepo, err := openRepository(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer closeRepo()

	opts := []service.Option{
		service.WithMaxInFlight(cfg.maxInFlight),
		service.WithSubmitTimeout(cfg.submitTimeout),
		service.WithLogger(logger),
	}
	if cfg.backoffBase > 0 {
		opts = append(opts, service.WithBackoff(service.ExponentialBackoff(cfg.backoffBase, cfg.backoffMax, rand.Float64)))
	}
	svc, err := service.NewService(repo, principal.NewHTTPClient(cfg.principalURL, nil), opts...)
	if err != nil {
		return err
	}

	handler := httpapi.NewHandler(svc, logger)
	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		recoverLoop(ctx, svc, cfg, logger)
	}()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.httpAddr, "max_in_flight", cfg.maxInFlight, "principal", cfg.principalURL)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
		close(serveErr)
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
	case <-ctx.Done():
	}

	// Graceful shutdown: stop accepting requests, cancel background processing so
	// claimed-but-unsent documents return to PENDING, then wait with a deadline.
	logger.Info("shutting down", "timeout", cfg.shutdownTimeout)
	sctx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()
	var errs []error
	if err := srv.Shutdown(sctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
	}
	if err := handler.Shutdown(sctx); err != nil {
		errs = append(errs, fmt.Errorf("worker shutdown: %w", err))
	}
	<-recoveryDone
	logger.Info("stopped")
	return errors.Join(errs...)
}

func openRepository(ctx context.Context, cfg config, logger *slog.Logger) (repository.Repository, func(), error) {
	if cfg.databaseURL == "" {
		logger.Warn("DATABASE_URL not set: using the in-memory repository (state is lost on exit)")
		return memory.New(), func() {}, nil
	}
	gdb, err := postgres.Open(cfg.databaseURL)
	if err != nil {
		return nil, nil, err
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, nil, fmt.Errorf("gorm sql db: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(pctx); err != nil {
		_ = sqlDB.Close()
		return nil, nil, fmt.Errorf("ping postgres: %w", err)
	}
	repo, err := postgres.New(gdb)
	if err != nil {
		_ = sqlDB.Close()
		return nil, nil, err
	}
	logger.Info("using GORM postgres repository")
	return repo, func() { _ = sqlDB.Close() }, nil
}

// recoverLoop periodically fails PROCESSING documents whose lease expired, so
// RetryFailed can pick them up. It stops when ctx ends.
func recoverLoop(ctx context.Context, svc *service.Service, cfg config, logger *slog.Logger) {
	if cfg.recoveryInterval <= 0 || cfg.leaseDuration <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.recoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := svc.RecoverStuck(ctx, cfg.leaseDuration); err != nil && ctx.Err() == nil {
				logger.Error("lease recovery failed", "error", err)
			}
		}
	}
}
