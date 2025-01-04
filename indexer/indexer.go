package indexer

import (
	"context"
	"flare-data-db-indexer/config"
	"flare-data-db-indexer/database"
	"flare-data-db-indexer/logger"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type BlockIndexer struct {
	destdb *gorm.DB
	params config.IndexerConfig
	srcdb  *gorm.DB
}

func CreateHistoryIndexer(cfg *config.Config, destdb *gorm.DB, srcdb *gorm.DB) (*BlockIndexer, error) {
	return &BlockIndexer{
		destdb: destdb,
		params: updateParams(cfg.Indexer),
		srcdb:  srcdb,
	}, nil
}

func updateParams(params config.IndexerConfig) config.IndexerConfig {
	if params.StopIndex == 0 {
		params.StopIndex = ^uint64(0)
	}

	params.BatchSize -= params.BatchSize % uint64(params.NumParallelReq)

	if params.LogRange == 0 {
		params.LogRange = 1
	}

	if params.BatchSize == 0 {
		params.BatchSize = 1
	}

	if params.NumParallelReq == 0 {
		params.NumParallelReq = 1
	}

	return params
}

func (ci *BlockIndexer) IndexHistory(ctx context.Context) error {
	states, err := database.UpdateDBStates(ctx, ci.destdb)
	if err != nil {
		return errors.Wrap(err, "database.UpdateDBStates")
	}

	ixRange, err := ci.getIndexRange(ctx, states)
	if err != nil {
		return err
	}

	logger.Info("Starting to index blocks from %d to %d", ixRange.start, ixRange.end)

	for i := ixRange.start; i <= ixRange.end; i = i + ci.params.BatchSize {
		if err := ci.indexBatch(ctx, states, i, ixRange); err != nil {
			return err
		}

		// in the second to last run of the loop update lastIndex to get the blocks
		// that were produced during the run of the algorithm
		if ci.shouldUpdateLastIndex(ixRange, i) {
			ixRange, err = ci.updateLastIndexHistory(ctx, states, ixRange)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (ci *BlockIndexer) indexBatch(
	ctx context.Context, states *database.DBStates, batchIx uint64, ixRange *indexRange,
) error {
	lastBlockNumInRound := min(batchIx+ci.params.BatchSize-1, ixRange.end)

	bBatch, err := ci.obtainBlocksBatch(ctx, batchIx, lastBlockNumInRound)
	if err != nil {
		return err
	}

	lastForDBTimestamp := uint64(bBatch.Histories[min(ci.params.BatchSize-1, ixRange.end-batchIx)].Timestamp)
	return ci.processAndSave(
		bBatch,
		states,
		lastBlockNumInRound,
		lastForDBTimestamp,
	)
}

func (ci *BlockIndexer) obtainBlocksBatch(
	ctx context.Context, firstBlockNumber uint64, lastBlockNumInRound uint64,
) (*databaseStructData, error) {
	startTime := time.Now()

	batchSize := lastBlockNumInRound + 1 - firstBlockNumber
	_ = batchSize
	bBatch := newDatabaseStructData()

	err := ci.srcdb.WithContext(ctx).Where("id BETWEEN ? AND ?", firstBlockNumber, lastBlockNumInRound).Find(bBatch.Histories).Error
	if err != nil {
		return nil, errors.Wrap(err, "Error fetching histories batch")
	}

	logger.Info(
		"Successfully obtained blocks %d to %d in %d milliseconds",
		firstBlockNumber, lastBlockNumInRound, time.Since(startTime).Milliseconds(),
	)

	return bBatch, nil
}

type indexRange struct {
	start uint64
	end   uint64
}

func (ci *BlockIndexer) getIndexRange(
	ctx context.Context, states *database.DBStates,
) (*indexRange, error) {
	lastHistoryIndex, lastHistoryTimestamp, err := ci.fetchLastHistoryIndex(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "ci.fetchLastHistoryIndex")
	}

	startHistoryIndex, startTimestamp, err := ci.fetchHistoryTimestamp(ctx, ci.params.StartIndex)
	if err != nil {
		return nil, errors.Wrap(err, "ci.fetchHistoryTimestamp")
	}

	startIndex, lastIndex, err := states.UpdateAtStart(ci.destdb, startHistoryIndex,
		startTimestamp, lastHistoryIndex, lastHistoryTimestamp, ci.params.StopIndex)
	if err != nil {
		return nil, errors.Wrap(err, "states.UpdateAtStart")
	}

	return &indexRange{start: startIndex, end: lastIndex}, nil
}

func (ci *BlockIndexer) updateLastIndexContinuous(
	ctx context.Context, states *database.DBStates, ixRange *indexRange,
) (*indexRange, error) {
	lastIndex, lastChainTimestamp, err := ci.fetchLastHistoryIndex(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "ci.fetchLastBlockIndex")
	}

	err = states.Update(ci.destdb, database.LastChainIndexState, lastIndex, lastChainTimestamp)
	if err != nil {
		return nil, errors.Wrap(err, "states.Update")
	}

	return &indexRange{start: ixRange.start, end: lastIndex}, nil
}

func (ci *BlockIndexer) processAndSave(
	bBatch *databaseStructData,
	states *database.DBStates,
	lastDBIndex, lastDBTimestamp uint64,
) error {
	startTime := time.Now()

	logger.Info(
		"Processed %d histories in %d milliseconds",
		len(bBatch.Histories),
		time.Since(startTime).Milliseconds(),
	)

	// Push transactions and logs in the database
	err := ci.saveData(bBatch, states, lastDBIndex, lastDBTimestamp)
	if err != nil {
		return errors.Wrap(err, "ci.saveData")
	}

	return nil
}

func (ci *BlockIndexer) shouldUpdateLastIndex(ixRange *indexRange, batchIx uint64) bool {
	return batchIx+ci.params.BatchSize <= ixRange.end && batchIx+2*ci.params.BatchSize > ixRange.end
}

func (ci *BlockIndexer) updateLastIndexHistory(
	ctx context.Context, states *database.DBStates, ixRange *indexRange,
) (*indexRange, error) {
	lastChainIndex, lastChainTimestamp, err := ci.fetchLastHistoryIndex(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "ci.fetchLastBlockIndex")
	}

	err = states.Update(ci.destdb, database.LastChainIndexState, lastChainIndex, lastChainTimestamp)
	if err != nil {
		return nil, errors.Wrap(err, "states.Update")
	}

	if lastChainIndex > ixRange.end && ci.params.StopIndex > ixRange.end {
		ixRange.end = min(lastChainIndex, ci.params.StopIndex)
		logger.Info("Updating the last block to %d", ixRange.end)
	}

	return ixRange, nil
}

func (ci *BlockIndexer) IndexContinuous(ctx context.Context) error {
	states, err := database.UpdateDBStates(ctx, ci.destdb)
	if err != nil {
		return errors.Wrap(err, "database.UpdateDBStates")
	}

	ixRange, err := ci.getIndexRange(ctx, states)
	if err != nil {
		return errors.Wrap(err, "ci.getIndexRange")
	}

	logger.Info("Continuously indexing blocks from %d", ixRange.start)

	// Request blocks one by one
	firstNum := ixRange.start
	for firstNum <= ci.params.StopIndex {
		if firstNum > ixRange.end {
			logger.Debug("Up to date, last block %d", states.States[database.LastChainIndexState].Index)
			time.Sleep(time.Millisecond * time.Duration(ci.params.NewBlockCheckMillis))

			ixRange, err = ci.updateLastIndexContinuous(ctx, states, ixRange)
			if err != nil {
				return err
			}

			continue
		}

		err = ci.indexContinuousIteration(ctx, states, ixRange, firstNum)
		if err != nil {
			return err
		}

		firstNum = ixRange.end + 1
	}

	logger.Debug("Stopping the indexer at block %d", states.States[database.LastDatabaseIndexState].Index)

	return nil
}

func (ci *BlockIndexer) indexContinuousIteration(
	ctx context.Context,
	states *database.DBStates,
	ixRange *indexRange,
	index uint64,
) error {
	bBatch, err := ci.obtainBlocksBatch(ctx, index, ixRange.end)
	if err != nil {
		return errors.Wrap(err, "ci.obtainBlocksBatch")
	}

	indexTimestamp := uint64(bBatch.Histories[len(bBatch.Histories)-1].Timestamp)
	err = ci.saveData(bBatch, states, index, indexTimestamp)
	if err != nil {
		return fmt.Errorf("IndexContinuous: %w", err)
	}

	if index%1000 == 0 {
		logger.Info("Indexer at block %d", index)
	}

	return nil
}
