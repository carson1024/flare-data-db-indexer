package srcdb

import (
	"context"
	"flare-data-db-indexer/config"
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func ConnectAndInitialize(ctx context.Context, cfg *config.DBConfig) (*gorm.DB, error) {
	db, err := connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("ConnectAndInitialize: Connect: %w", err)
	}

	return db, nil
}

func connect(ctx context.Context, cfg *config.DBConfig) (*gorm.DB, error) {
	// Connect to the database
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable", cfg.Host, cfg.Username, cfg.Password, cfg.Database, cfg.Port)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return db.WithContext(ctx), nil
}
