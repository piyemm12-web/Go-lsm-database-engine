package main

import (
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"my-kv-store/internal/db"
	"my-kv-store/internal/middleware"
	"my-kv-store/internal/server"
)

//go:embed static/*
var staticFS embed.FS

func main() {
	port := flag.Int("port", 8080, "HTTP server listening port")
	dataDir := flag.String("data-dir", "./data", "Directory path for LSM-tree database SSTables & WAL")
	strategy := flag.String("strategy", "sliding_window", "Default rate limiter strategy: sliding_window, leaky_bucket, token_bucket")
	limit := flag.Int64("limit", 10, "Max request limit")
	windowSec := flag.Int64("window", 60, "Time window duration in seconds")
	flag.Parse()

	fmt.Println("==========================================================")
	fmt.Println("🚀 Initializing Key-Value LSM-Tree Engine & REST Server")
	fmt.Println("==========================================================")

	opts := db.DefaultOptions(*dataDir)
	database, err := db.Open(opts)
	if err != nil {
		log.Fatalf("Failed to open LSM key-value database: %v", err)
	}
	defer func() {
		fmt.Println("Shutting down LSM database...")
		database.Close()
	}()

	fmt.Printf("✓ Database ready at directory: %s\n", *dataDir)

	strat := middleware.StrategyType(*strategy)
	limiter := middleware.NewIPRateLimiter(strat, *limit, time.Duration(*windowSec)*time.Second)
	fmt.Printf("✓ Rate Limiter active strategy: %s (limit: %d, window: %ds)\n", *strategy, *limit, *windowSec)

	srv := server.NewServer(database, limiter)

	// Serve embedded Web UI Dashboard
	subFS, err := fs.Sub(staticFS, "static")
	if err == nil {
		srv.RegisterStatic(http.FileServer(http.FS(subFS)))
		fmt.Println("✓ Web Dashboard enabled at http://localhost:", *port)
	}

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: srv,
	}

	// Graceful shutdown handling
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		fmt.Println("\nReceived termination signal. Closing server...")
		httpServer.Close()
	}()

	fmt.Printf("\n⚡ KV REST Server listening on http://localhost:%d\n", *port)
	fmt.Println("Press Ctrl+C to stop.")
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}
