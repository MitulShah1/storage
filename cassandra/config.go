package cassandra

import (
	"time"

	"github.com/gocql/gocql"
)

// Config defines the configuration options for the Cassandra storage
type Config struct {
	// Optional. Default is localhost
	// Hosts is a list of Cassandra nodes to connect to.
	Hosts []string
	// Optional. Default is gofiber
	// Keyspace is the name of the Cassandra keyspace to use.
	Keyspace string
	// Optional. Default is kv_store
	/// Table is the name of the Cassandra table to use.
	Table string
	// Optional. Default is Quorum
	// Consistency is the Cassandra consistency level.
	Consistency gocql.Consistency
	// Optional. Default is 10 minutes
	// Expiration is the time after which an entry is considered expired.
	Expiration time.Duration
	// Optional. Default is false
	// Reset is a flag to reset the database.
	Reset bool
}

// ConfigDefault is the default config
var ConfigDefault = Config{
	Hosts:       []string{"localhost:9042"},
	Keyspace:    "gofiber",
	Table:       "kv_store",
	Consistency: gocql.Quorum,
	Reset:       false,
	Expiration:  10 * time.Minute,
}

// ConfigDefault is the Helper function to apply default config
func configDefault(config ...Config) Config {
	// Return default config if nothing provided
	if len(config) < 1 {
		return ConfigDefault
	}

	// Override default config
	cfg := config[0]

	// Set default values
	if len(cfg.Hosts) == 0 {
		cfg.Hosts = ConfigDefault.Hosts
	}

	if cfg.Keyspace == "" {
		cfg.Keyspace = ConfigDefault.Keyspace
	}

	if cfg.Table == "" {
		cfg.Table = ConfigDefault.Table
	}

	if cfg.Consistency == 0 {
		cfg.Consistency = ConfigDefault.Consistency
	}

	if cfg.Expiration == 0 {
		cfg.Expiration = ConfigDefault.Expiration
	}

	return cfg
}
