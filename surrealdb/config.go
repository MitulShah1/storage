package surrealdb

import "time"

// Config defines the config for storage.
type Config struct {
	// Endpoint for SurrealDB
	//
	// Optional. Default is "ws://localhost:8000/rpc"
	Endpoint string

	// Namespace for SurrealDB
	//
	// Optional. Default is "fiber"
	Namespace string

	// Database name
	//
	// Optional. Default is "fiber"
	Database string

	// Table name
	//
	// Optional. Default is "fiber_storage"
	Table string

	// Username for authentication
	//
	// Optional. Default is "root"
	Username string

	// Password for authentication
	//
	// Optional. Default is "root"
	Password string

	// Reset clears any existing keys in existing Table
	//
	// Optional. Default is false
	Reset bool

	// Time before deleting expired keys
	//
	// Optional. Default is 10 * time.Second
	GCInterval time.Duration
}

// ConfigDefault is the default config
var ConfigDefault = Config{
	Endpoint:   "ws://localhost:8000/rpc",
	Namespace:  "fiber",
	Database:   "fiber",
	Table:      "fiber_storage",
	Username:   "root",
	Password:   "root",
	Reset:      false,
	GCInterval: 10 * time.Second,
}

func configDefault(config ...Config) Config {

	// Return default config if nothing provided
	if len(config) < 1 {
		return ConfigDefault
	}

	// Override default config
	cfg := config[0]

	// Set default values
	if cfg.Endpoint == "" {
		cfg.Endpoint = ConfigDefault.Endpoint
	}
	if cfg.Namespace == "" {
		cfg.Namespace = ConfigDefault.Namespace
	}
	if cfg.Database == "" {
		cfg.Database = ConfigDefault.Database
	}
	if cfg.Table == "" {
		cfg.Table = ConfigDefault.Table
	}
	if cfg.Username == "" {
		cfg.Username = ConfigDefault.Username
	}
	if cfg.Password == "" {
		cfg.Password = ConfigDefault.Password
	}
	if cfg.GCInterval == 0 {
		cfg.GCInterval = ConfigDefault.GCInterval
	}

	return cfg
}
