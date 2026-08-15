package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
	"github.com/munisp/blueeconomy-port-interoperability/internal/server"
)

func main() {
	if err := run(); err != nil {
		log.Printf("port-interoperability: %v", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL := requiredEnv("DATABASE_URL")
	migrationPaths := requiredEnv("MIGRATION_PATH")
	port := requiredEnv("PORT")
	authMode := requiredEnv("AUTH_MODE")
	if authMode != server.AuthModeLoopbackTrustedProxy {
		return fmt.Errorf("AUTH_MODE must be %q until a verified Ministry OIDC edge is configured", server.AuthModeLoopbackTrustedProxy)
	}
	paths := strings.Split(migrationPaths, ",")
	migrations := make([][]byte, 0, len(paths))
	for _, migrationPath := range paths {
		migrationPath = strings.TrimSpace(migrationPath)
		if migrationPath == "" {
			return errors.New("MIGRATION_PATH contains an empty path")
		}
		migration, readErr := os.ReadFile(filepath.Clean(migrationPath))
		if readErr != nil {
			return fmt.Errorf("read migration %q: %w", migrationPath, readErr)
		}
		migrations = append(migrations, migration)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	store, err := portcall.Open(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	for index, migration := range migrations {
		if _, err := store.Exec(ctx, string(migration)); err != nil {
			return fmt.Errorf("apply migration %d: %w", index+1, err)
		}
	}
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           server.New(store, authMode),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}()
	log.Printf("port-interoperability listening on :%s", port)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func requiredEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s must be set", name)
	}
	return value
}
