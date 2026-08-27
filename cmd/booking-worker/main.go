// booking-worker runs the Temporal worker for ECallUpBookingWorkflow. It fails
// closed unless Temporal, PostgreSQL and TigerBeetle are all configured.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/ledger"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	if err := run(); err != nil {
		log.Printf("booking-worker: %v", err)
		os.Exit(1)
	}
}

func run() error {
	address := requiredEnv("TEMPORAL_ADDRESS")
	namespace := requiredEnv("TEMPORAL_NAMESPACE")
	taskQueue := requiredEnv("TEMPORAL_TASK_QUEUE")
	databaseURL := requiredEnv("DATABASE_URL")
	clusterID := requiredEnv("TIGERBEETLE_CLUSTER_ID")
	addresses := strings.Split(requiredEnv("TIGERBEETLE_ADDRESSES"), ",")

	settlement, err := ledger.NewTigerBeetle(clusterID, addresses)
	if err != nil {
		return fmt.Errorf("configure TigerBeetle ledger: %w", err)
	}
	defer settlement.Close()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}
	defer pool.Close()

	activities, err := booking.NewActivities(booking.NewStore(pool), settlement)
	if err != nil {
		return err
	}
	temporalClient, err := client.Dial(client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return fmt.Errorf("dial temporal: %w", err)
	}
	defer temporalClient.Close()

	bookingWorker := worker.New(temporalClient, taskQueue, worker.Options{})
	bookingWorker.RegisterWorkflow(booking.ECallUpBookingWorkflow)
	bookingWorker.RegisterActivity(activities)
	log.Printf("booking-worker polling task queue %s", taskQueue)
	if err := bookingWorker.Run(worker.InterruptCh()); err != nil {
		return fmt.Errorf("run worker: %w", err)
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
