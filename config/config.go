package config

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

var (
	BackoffMaxElapsedTime time.Duration                = 5 * time.Minute
	Timeout               time.Duration                = 1000 * time.Millisecond
	GlobalConfigCallback  ConfigCallback[GlobalConfig] = ConfigCallback[GlobalConfig]{}
	CfgFlag                                            = flag.String("config", "config.toml", "Configuration file (toml format)")
)

func init() {
	GlobalConfigCallback.AddCallback(func(config GlobalConfig) {
		tCfg := config.TimeoutConfig()

		if tCfg.BackoffMaxElapsedTimeSeconds != nil {
			BackoffMaxElapsedTime = time.Duration(*tCfg.BackoffMaxElapsedTimeSeconds) * time.Second
		}

		if tCfg.TimeoutMillis > 0 {
			Timeout = time.Duration(tCfg.TimeoutMillis) * time.Millisecond
		}
	})
}

type GlobalConfig interface {
	LoggerConfig() LoggerConfig
	TimeoutConfig() TimeoutConfig
}

type Config struct {
	SrcDB   DBConfig      `toml:"srcdb"`
	DestDB  DBConfig      `toml:"destdb"`
	Logger  LoggerConfig  `toml:"logger"`
	Indexer IndexerConfig `toml:"indexer"`
	Timeout TimeoutConfig `toml:"timeout"`
}

type LoggerConfig struct {
	Level       string `toml:"level"` // valid values are: DEBUG, INFO, WARN, ERROR, DPANIC, PANIC, FATAL (zap)
	File        string `toml:"file"`
	MaxFileSize int    `toml:"max_file_size"` // In megabytes
	Console     bool   `toml:"console"`
}

type DBConfig struct {
	Host             string `toml:"host"`
	Port             int    `toml:"port"`
	Database         string `toml:"database"`
	Schema           string `toml:"schema"`
	Username         string `toml:"username"`
	Password         string `toml:"password"`
	LogQueries       bool   `toml:"log_queries"`
	HistoryDrop      uint64 `toml:"history_drop"`
	DropTableAtStart bool   `toml:"drop_table_at_start"`
}

type IndexerConfig struct {
	BatchSize           uint64 `toml:"batch_size"`
	StartIndex          uint64 `toml:"start_index"`
	StopIndex           uint64 `toml:"stop_index"`
	NumParallelReq      int    `toml:"num_parallel_req"`
	LogRange            uint64 `toml:"log_range"`
	NewBlockCheckMillis int    `toml:"new_block_check_millis"`
}

type TimeoutConfig struct {
	BackoffMaxElapsedTimeSeconds *int `toml:"backoff_max_elapsed_time_seconds"`
	TimeoutMillis                int  `toml:"timeout_millis"`
}

func BuildConfig() (*Config, error) {
	cfgFileName := *CfgFlag

	cfg := &Config{Indexer: IndexerConfig{}}
	err := parseConfigFile(cfg, cfgFileName)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func parseConfigFile(cfg *Config, fileName string) error {
	content, err := os.ReadFile(fileName)
	if err != nil {
		return fmt.Errorf("error opening config file: %w", err)
	}

	_, err = toml.Decode(string(content), cfg)
	if err != nil {
		return fmt.Errorf("error parsing config file: %w", err)
	}
	return nil
}

func (c Config) LoggerConfig() LoggerConfig {
	return c.Logger
}

func (c Config) TimeoutConfig() TimeoutConfig {
	return c.Timeout
}
