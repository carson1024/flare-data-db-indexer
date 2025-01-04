package main

import (
	"context"
	"flag"
	"flare-data-db-indexer/config"
	"flare-data-db-indexer/database"
	"flare-data-db-indexer/indexer"
	"flare-data-db-indexer/logger"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

func main() {
	if err := run(context.Background()); err != nil {
		logger.Fatal("Fatal error: %s", err)
	}
}

func run(ctx context.Context) error {
	flag.Parse()
	cfg, err := config.BuildConfig()
	if err != nil {
		return errors.Wrap(err, "config error")
	}

	config.GlobalConfigCallback.Call(cfg)

	srcdb, err := database.Connect(ctx, &cfg.SrcDB)
	if err != nil {
		return errors.Wrap(err, "Source Database connect and initialize errors")
	}

	destdb, err := database.ConnectAndInitialize(ctx, &cfg.DestDB)
	if err != nil {
		return errors.Wrap(err, "Destination Database connect and initialize errors")
	}

	if cfg.DestDB.HistoryDrop > 0 {
		// Run an initial iteration of the history drop. This could take some
		// time if it has not been run in a while after an outage - running
		// separately avoids database clashes with the indexer.
		logger.Info("running initial DropHistory iteration")
		startTime := time.Now()

		var firstBlockNumber uint64

		err = backoff.RetryNotify(
			func() (err error) {
				firstBlockNumber, err = database.DropHistoryIteration(ctx, destdb, cfg.DestDB.HistoryDrop, srcdb)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil
				}

				return err
			},
			backoff.NewExponentialBackOff(),
			func(err error, d time.Duration) {
				logger.Error("DropHistory error: %s. Will retry after %s", err, d)
			},
		)
		if err != nil {
			return errors.Wrap(err, "startup DropHistory error")
		}

		logger.Info("initial DropHistory iteration finished in %s", time.Since(startTime))

		if firstBlockNumber > cfg.Indexer.StartIndex {
			logger.Info("Setting new startIndex due to history drop: %d", firstBlockNumber)
			cfg.Indexer.StartIndex = firstBlockNumber
		}
	}

	return runIndexer(ctx, cfg, destdb, srcdb)
}

func runIndexer(ctx context.Context, cfg *config.Config, destdb *gorm.DB, srcdb *gorm.DB) error {
	cIndexer, err := indexer.CreateHistoryIndexer(cfg, destdb, srcdb)
	if err != nil {
		return err
	}

	bOff := backoff.NewExponentialBackOff()

	err = backoff.RetryNotify(
		func() error {
			return cIndexer.IndexHistory(ctx)
		},
		bOff,
		func(err error, d time.Duration) {
			logger.Error("Index history error: %s. Will retry after %s", err, d)
		},
	)
	if err != nil {
		return errors.Wrap(err, "Index history fatal error")
	}

	if cfg.DestDB.HistoryDrop > 0 {
		go database.DropHistory(
			ctx, destdb, cfg.DestDB.HistoryDrop, database.HistoryDropIntervalCheck, srcdb,
		)
	}

	err = backoff.RetryNotify(
		func() error {
			return cIndexer.IndexContinuous(ctx)
		},
		bOff,
		func(err error, d time.Duration) {
			logger.Error("Index continuous error: %s. Will retry after %s", err, d)
		},
	)
	if err != nil {
		return errors.Wrap(err, "Index continuous fatal error")
	}

	logger.Info("Finished indexing")

	return nil
}
