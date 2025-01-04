package database

import (
	"context"
	"flare-data-db-indexer/logger"
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func DropHistory(
	ctx context.Context, destdb *gorm.DB, intervalSeconds, checkInterval uint64, srcdb *gorm.DB,
) {
	for {
		logger.Info("starting DropHistory iteration")

		startTime := time.Now()
		_, err := DropHistoryIteration(ctx, destdb, intervalSeconds, srcdb)
		if err == nil || errors.Is(err, gorm.ErrRecordNotFound) {
			duration := time.Since(startTime)
			logger.Info("finished DropHistory iteration in %v", duration)
		} else {
			logger.Error("DropHistory error: %s", err)
		}

		time.Sleep(time.Duration(checkInterval) * time.Second)
	}
}

var deleteOrder []interface{} = []interface{}{
	MarketHistory{},
}

// Only delete up to 1000 items in a single DB transaction to avoid lock
// timeouts.
const deleteBatchSize = 1000

func DropHistoryIteration(
	ctx context.Context, destdb *gorm.DB, intervalSeconds uint64, srcdb *gorm.DB,
) (uint64, error) {
	_, lastBlockTime, err := getBlockTimestamp(ctx, srcdb)
	if err != nil {
		return 0, errors.Wrap(err, "Failed to get the latest time")
	}

	deleteStart := lastBlockTime - intervalSeconds

	destdb = destdb.WithContext(ctx)

	// Delete in specified order to not break foreign keys.
	for _, entity := range deleteOrder {
		if err := deleteInBatches(destdb, deleteStart, entity); err != nil {
			return 0, err
		}
	}

	var firstBlockNumber uint64
	err = destdb.Transaction(func(tx *gorm.DB) error {
		var firstBlock MarketHistory
		err = tx.Order("id").First(&firstBlock).Error
		if err != nil {
			return errors.Wrap(err, "Failed to get first block in the DB")
		}

		firstBlockNumber = firstBlock.ID

		err = globalStates.Update(tx, FirstDatabaseIndexState, firstBlockNumber, firstBlock.Timestamp)
		if err != nil {
			return errors.Wrap(err, "Failed to update state in the DB")
		}

		logger.Info("Deleted blocks up to index %d", firstBlock.ID)

		return nil
	})

	return firstBlockNumber, err
}

func deleteInBatches(db *gorm.DB, deleteStart uint64, entity interface{}) error {
	for {
		result := db.Limit(deleteBatchSize).Where("timestamp < ?", deleteStart).Delete(&entity)

		if result.Error != nil {
			return errors.Wrap(result.Error, "Failed to delete historic data in the DB")
		}

		if result.RowsAffected == 0 {
			return nil
		}
	}
}

func getBlockTimestamp(ctx context.Context, srcdb *gorm.DB) (uint64, uint64, error) {
	lastHistory := new(*MarketHistory)
	err := srcdb.WithContext(ctx).Order(clause.OrderByColumn{Column: clause.Column{Name: "id"}, Desc: true}).First(lastHistory).Error
	if err != nil {
		return 0, 0, errors.Wrap(err, "fetchLastHistoryIndex lastHistory")
	}

	return (**lastHistory).ID, uint64((**lastHistory).Timestamp), nil
}
