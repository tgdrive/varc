package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/tgdrive/varc/httpcache"

	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	port         = pflag.String("port", "8080", "Port to listen on")
	cacheDir     = pflag.String("cache-dir", filepath.Join(os.TempDir(), "varc_cache"), "Cache directory")
	chunkSize    = pflag.String("chunk-size", "", "Chunk size for reading (e.g., 4M)")
	chunkStreams = pflag.Int("chunk-streams", 2, "Number of parallel chunk streams")
	stripQuery   = pflag.Bool("strip-query", false, "Strip query parameters from URL for caching")
	stripDomain  = pflag.Bool("strip-domain", false, "Strip domain from URL for caching")
	shardLevel   = pflag.Int("shard-level", 1, "Number of shard levels for cache paths")
)

func main() {
	pflag.Parse()

	zapLogger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer zapLogger.Sync()

	opt := httpcache.Options{
		CacheDir:          *cacheDir,
		CacheChunkSize:    *chunkSize,
		CacheChunkStreams: *chunkStreams,
		StripQuery:        *stripQuery,
		StripDomain:       *stripDomain,
		ShardLevel:        *shardLevel,
		Logger:            zapLogger.Sugar(),
	}

	handler, err := httpcache.NewHandler(opt)
	if err != nil {
		zapLogger.Fatal("Failed to create handler", zap.Error(err))
	}

	mux := http.NewServeMux()

	mainHandler := func(w http.ResponseWriter, r *http.Request) {
		targetURL := r.URL.Query().Get("url")

		// Check for Base64 URL in path
		if targetURL == "" && strings.HasPrefix(r.URL.Path, "/stream/") {
			encodedURL := strings.TrimPrefix(r.URL.Path, "/stream/")
			if decoded, err := base64.RawURLEncoding.DecodeString(encodedURL); err == nil {
				targetURL = string(decoded)
			} else if decoded, err := base64.URLEncoding.DecodeString(encodedURL); err == nil {
				targetURL = string(decoded)
			}
		}

		if targetURL == "" {
			http.Error(w, "Missing 'url' parameter or base64 path", http.StatusBadRequest)
			return
		}

		handler.Serve(w, r, targetURL)
	}

	mux.HandleFunc("/stream", mainHandler)
	mux.HandleFunc("/stream/", mainHandler)

	// Health check endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"ok","cache_dir":"%s"}`, handler.Engine.Opt.CacheDir)
	})

	srv := &http.Server{
		Addr:    ":" + *port,
		Handler: mux,
	}

	// Channel to listen for signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		zapLogger.Info("Engine listening",
			zap.String("addr", ":"+*port),

			zap.String("cache_dir", handler.Engine.Opt.CacheDir),
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			zapLogger.Fatal("Listen error", zap.Error(err))
		}
	}()

	<-stop

	zapLogger.Info("Shutting down gracefully...")

	// Create a context with timeout for the shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		zapLogger.Fatal("Server forced to shutdown", zap.Error(err))
	}

	zapLogger.Info("Shutting down handler...")
	handler.Shutdown()

	zapLogger.Info("Exit")
}
