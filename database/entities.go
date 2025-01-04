package database

// BaseEntity is an abstract entity, all other entities should be derived from it
type BaseEntity struct {
	ID uint64 `gorm:"primaryKey;unique"`
}

// MarketHistory represents the `flare.market_history` table in GORM
type MarketHistory struct {
	BaseEntity
	Market    string  `gorm:"type:varchar(30);not null"`
	Source    int16   `gorm:"not null"`
	Timestamp uint64  `gorm:"not null"`
	Tickers   *string `gorm:"type:text"` // Nullable, use pointer
}
