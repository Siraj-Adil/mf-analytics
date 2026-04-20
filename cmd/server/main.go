// Command server starts the Mutual Fund Analytics HTTP API.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mutual-fund-analytics/internal/analytics"
	"github.com/mutual-fund-analytics/internal/api"
	"github.com/mutual-fund-analytics/internal/config"
	"github.com/mutual-fund-analytics/internal/db"
	"github.com/mutual-fund-analytics/internal/mfapi"
	"github.com/mutual-fund-analytics/internal/pipeline"
	"github.com/mutual-fund-analytics/internal/ratelimiter"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("[main] starting mutual-fund-analytics service")

	// Load configuration.
	cfg := config.Load()

	// Open database.
	database, err := db.New(cfg.DBPath)
	if err != nil {
		log.Fatalf("[main] open database: %v", err)
	}
	defer database.Close()

	// Initialise rate limiter (loads persisted state for crash recovery).
	limiter, err := ratelimiter.New(
		cfg.RateLimitPerSecond,
		cfg.RateLimitPerMinute,
		cfg.RateLimitPerHour,
		database,
	)
	if err != nil {
		log.Fatalf("[main] init rate limiter: %v", err)
	}
	log.Printf("[main] rate limiter: %d/sec, %d/min, %d/hour",
		cfg.RateLimitPerSecond, cfg.RateLimitPerMinute, cfg.RateLimitPerHour)

	// Initialise mfapi client.
	client := mfapi.NewClient(cfg.MFAPIBase, limiter)

	// Initialise analytics engine.
	engine := analytics.New(database)

	// Initialise pipeline.
	pipe := pipeline.New(cfg, database, client, engine)

	// Build HTTP handler and router.
	handler := api.NewHandler(database, pipe, engine, limiter)
	router := api.NewRouter(handler)

	// HTTP server.
	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown context.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start background auto-sync on startup.
	go func() {
		// Give the server a moment to start.
		time.Sleep(2 * time.Second)
		log.Printf("[main] starting background sync on boot…")
		if err := pipe.Run(ctx, "auto"); err != nil {
			log.Printf("[main] background sync: %v", err)
		}
	}()

	// Start daily incremental sync ticker.
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Printf("[main] running scheduled daily sync…")
				if err := pipe.Run(ctx, "scheduled"); err != nil {
					log.Printf("[main] scheduled sync: %v", err)
				}
			}
		}
	}()

	// Signal handling for graceful shutdown.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Printf("[main] shutdown signal received")
		cancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("[main] server shutdown error: %v", err)
		}
	}()

	log.Printf("[main] HTTP server listening on :%s", cfg.Port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[main] server error: %v", err)
	}
	log.Printf("[main] server stopped")
}
