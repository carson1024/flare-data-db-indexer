package indexer

import (
	"flare-data-db-indexer/database"

	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type databaseStructData struct {
	Histories []*database.MarketHistory
}

func newDatabaseStructData() *databaseStructData {
	return &databaseStructData{
		Histories: []*database.MarketHistory{},
	}
}

func (ci *BlockIndexer) saveData(
	data *databaseStructData, states *database.DBStates, lastDBIndex, lastDBTimestamp uint64,
) error {
	return ci.destdb.Transaction(func(tx *gorm.DB) error {
		if len(data.Histories) != 0 {
			err := tx.Clauses(clause.OnConflict{DoNothing: true}).
				CreateInBatches(data.Histories, database.DBTransactionBatchesSize).
				Error
			if err != nil {
				return errors.Wrap(err, "saveData: CreateInBatches0")
			}
		}

		err := states.Update(tx, database.LastDatabaseIndexState, lastDBIndex, lastDBTimestamp)
		if err != nil {
			return errors.Wrap(err, "saveData: Update")
		}

		return nil
	})
}
