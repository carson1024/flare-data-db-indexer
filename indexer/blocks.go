package indexer

import (
	"context"
	"flare-data-db-indexer/database"

	"github.com/pkg/errors"
	"gorm.io/gorm/clause"
)

func (ci *BlockIndexer) fetchLastHistoryIndex(ctx context.Context) (uint64, uint64, error) {
	lastHistory := new(*database.MarketHistory)
	err := ci.srcdb.WithContext(ctx).Order(clause.OrderByColumn{Column: clause.Column{Name: "id"}, Desc: true}).First(lastHistory).Error
	if err != nil {
		return 0, 0, errors.Wrap(err, "fetchLastHistoryIndex lastHistory")
	}

	return (**lastHistory).ID, uint64((**lastHistory).Timestamp), nil
}

func (ci *BlockIndexer) fetchHistoryTimestamp(ctx context.Context, index uint64) (uint64, uint64, error) {
	firstHistory := new(*database.MarketHistory)
	err := ci.srcdb.WithContext(ctx).Where("id >= ?", index).First(firstHistory).Error
	if err != nil {
		return 0, 0, errors.Wrap(err, "fetchHistoryTimestamp lastHistory")
	}

	return (**firstHistory).ID, uint64((**firstHistory).Timestamp), nil
}
