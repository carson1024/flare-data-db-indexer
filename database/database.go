package database

import (
	"context"
	"flare-data-db-indexer/config"
	"fmt"
	"sync/atomic"

	"github.com/pkg/errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

const (
	tcp                      = "tcp"
	HistoryDropIntervalCheck = 60 * 30 // every 30 min
	DBTransactionBatchesSize = 1000
)

var (
	// List entities to auto-migrate
	entities = []interface{}{
		State{},
		MarketHistory{},
	}
	HistoryId atomic.Uint64
)

func ConnectAndInitialize(ctx context.Context, cfg *config.DBConfig) (*gorm.DB, error) {
	db, err := connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("ConnectAndInitialize: Connect: %w", err)
	}

	if cfg.DropTableAtStart {
		err = db.Migrator().DropTable(entities...)
		if err != nil {
			return nil, err
		}
	}

	// Initialize - auto migrate
	err = db.AutoMigrate(entities...)
	if err != nil {
		return nil, errors.Wrap(err, "ConnectAndInitialize: AutoMigrate")
	}

	// If the state info is not in the DB, create it
	_, err = UpdateDBStates(ctx, db)
	if err != nil {
		for _, name := range stateNames {
			s := &State{Name: name}
			s.updateIndex(0, 0)
			err = db.Create(s).Error
			if err != nil {
				return nil, errors.Wrap(err, "ConnectAndInitialize: Create")
			}
		}
	}

	if err := storeTransactionID(db); err != nil {
		return nil, err
	}

	return db, nil
}

func Connect(ctx context.Context, cfg *config.DBConfig) (*gorm.DB, error) {
	db, err := connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("Connect: %w", err)
	}

	return db, nil
}

func storeTransactionID(db *gorm.DB) (err error) {
	maxIndexTx := new(MarketHistory)
	err = db.Last(maxIndexTx).Error
	if err == nil {
		HistoryId.Store(maxIndexTx.ID + 1)
		return nil
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		HistoryId.Store(1)
		return nil
	}

	return errors.Wrap(err, "Failed to obtain ID data from DB")
}

func connect(ctx context.Context, cfg *config.DBConfig) (*gorm.DB, error) {
	// Connect to the database
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d search_path=%s sslmode=disable", cfg.Host, cfg.Username, cfg.Password, cfg.Database, cfg.Port, cfg.Schema)

	gormLogLevel := getGormLogLevel(cfg)
	gormConfig := gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   cfg.Schema + ".", // Add schema prefix to all tables
			SingularTable: true,             // Use singular table names
		},
		Logger:          gormlogger.Default.LogMode(gormLogLevel),
		CreateBatchSize: DBTransactionBatchesSize,
	}

	db, err := gorm.Open(postgres.Open(dsn), &gormConfig)
	if err != nil {
		return nil, err
	}

	return db.WithContext(ctx), nil
}

func getGormLogLevel(cfg *config.DBConfig) gormlogger.LogLevel {
	if cfg.LogQueries {
		return gormlogger.Info
	}

	return gormlogger.Silent
}
