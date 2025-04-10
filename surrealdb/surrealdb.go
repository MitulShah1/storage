package surrealdb

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/connection"
)

type SchemaDefinition struct {
	Events  map[string]interface{} `json:"events"`
	Fields  map[string]string      `json:"fields"`
	Indexes map[string]interface{} `json:"indexes"`
	Lives   map[string]interface{} `json:"lives"`
	Tables  map[string]interface{} `json:"tables"`
}

// Storage interface that is implemented by storage drivers
type Storage struct {
	db            *surrealdb.DB
	config        Config
	surrealSelect string
	surrealInsert string
	surrealDelete string
	surrealReset  string
	sqlGC         string
}

type DBResponse struct {
	ID     any                  `json:"id" msgpack:"id"`
	Error  *connection.RPCError `json:"error,omitempty" msgpack:"error,omitempty"`
	Result any
}

type FieldDefinition struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Response[T any] struct {
	Time   string `json:"time"`
	Status string `json:"status"`
	Result []T    `json:"result"`
}

type TableInfo struct {
	Name       string            `json:"name"`
	SchemaFull bool              `json:"schemafull"`
	Fields     []FieldDefinition `json:"fields"`
}

// QueryResponse defines the structure of the query response.
type QueryResponse struct {
	ID     any                  `json:"id" msgpack:"id"`
	Error  *connection.RPCError `json:"error,omitempty" msgpack:"error,omitempty"`
	Result *[]QueryResult       `json:"result,omitempty" msgpack:"result,omitempty"`
}

// QueryResult represents each query result from SurrealDB.
type QueryResult struct {
	Status string `json:"status"`
	Time   string `json:"time"`
	Result any    `json:"result"`
}

var (
	initQuery = `DEFINE TABLE %s SCHEMAFULL;
		 DEFINE FIELD k ON %s TYPE string;
		 DEFINE FIELD v ON %s TYPE string;
		 DEFINE FIELD e ON %s TYPE int;`
	checkSchemaQuery = `INFO FOR TABLE %s;`
)

// StorageEntry represents a single entry in the storage table
type StorageEntry struct {
	ID string `json:"id"`
	K  string `json:"k"`
	V  string `json:"v"`
	E  int64  `json:"e"`
}

// New creates a new storage
func New(config ...Config) (*Storage, error) {

	// Set default config
	cfg := configDefault(config...)

	// Create new storage
	store := &Storage{
		config:        cfg,
		surrealSelect: fmt.Sprintf("SELECT * FROM %s WHERE k = $key", cfg.Table),
		surrealInsert: fmt.Sprintf("INSERT INTO %s (k, v, e) VALUES ($key, $value, $expiry)", cfg.Table),
		surrealDelete: fmt.Sprintf("DELETE FROM %s WHERE k = $key", cfg.Table),
		surrealReset:  fmt.Sprintf("REMOVE TABLE %s", cfg.Table),
		sqlGC:         fmt.Sprintf("DELETE FROM %s WHERE e <= $expiry AND e != 0", cfg.Table),
	}

	// Connect to database
	db, err := surrealdb.New(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
	}

	// Use namespace and database
	if err := db.Use(cfg.Namespace, cfg.Database); err != nil {
		return nil, fmt.Errorf("failed to use namespace and database: %w", err)
	}

	// Sign in to authentication `db`
	authData := &surrealdb.Auth{
		Username: cfg.Username, // use your setup username
		Password: cfg.Password, // use your setup password
	}

	token, err := db.SignIn(authData)
	if err != nil {
		return nil, fmt.Errorf("failed to sign in to SurrealDB: %w", err)
	}

	// Check token validity. This is not necessary if you called `SignIn` before. This authenticates the `db` instance too if sign in was
	// not previously called
	if err := db.Authenticate(token); err != nil {
		return nil, fmt.Errorf("failed to authenticate SurrealDB: %w", err)
	}

	// Set database
	store.db = db

	// Reset if requested
	if cfg.Reset {
		if err := store.Reset(); err != nil {
			return nil, fmt.Errorf("failed to reset: %w", err)
		}
	}

	// // Initialize the storage
	if err := store.initQuery(); err != nil {
		return nil, fmt.Errorf("failed to init query: %w", err)
	}

	// Check schema
	if err := store.checkSchema(); err != nil {
		return nil, err
	}

	// // Start garbage collector
	// if cfg.GCInterval > 0 {
	// 	go store.gc()
	// }

	return store, nil
}

func (s *Storage) initQuery() error {
	var res Response[map[string]interface{}]
	query := fmt.Sprintf(initQuery, s.config.Table, s.config.Table, s.config.Table, s.config.Table)
	if err := s.db.Send(&res, "query", query, nil); err != nil {
		return fmt.Errorf("failed to send query: %w", err)
	}
	return nil
}

func (s *Storage) checkSchema() error {
	var res Response[map[string]interface{}]
	if err := s.db.Send(&res, "query", fmt.Sprintf(checkSchemaQuery, s.config.Table), nil); err != nil {
		return fmt.Errorf("Error checking schema: %w", err)
	}
	return nil
}

// Get value by key
func (s *Storage) Get(key string) ([]byte, error) {
	var res QueryResponse

	//params := map[string]interface{}{"key": key}
	if err := s.db.Send(&res, "query", "SELECT * from storage where k='key2'"); err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}

	if res.Result == nil || len(*res.Result) == 0 {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	result := (*res.Result)[0].Result
	entries, ok := result.([]interface{})
	if !ok || len(entries) == 0 {
		return nil, fmt.Errorf("invalid response format or empty result")
	}

	entryMap, ok := entries[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid entry format")
	}

	// Check if entry is expired
	if e, exists := entryMap["e"].(float64); exists && e != 0 {
		expiry := int64(e)
		if expiry <= time.Now().Unix() {
			// Entry is expired, delete it
			_ = s.Delete(key)
			return nil, fmt.Errorf("key expired: %s", key)
		}
	}

	if val, exists := entryMap["v"].(string); exists {
		return []byte(val), nil
	}

	return nil, fmt.Errorf("value not found for key: %s", key)
}

// Alternative Get implementation using direct Send method
func (s *Storage) GetAlt(key string) ([]byte, error) {
	var res interface{}

	query := fmt.Sprintf("SELECT * FROM %s WHERE k = $key", s.config.Table)
	params := map[string]interface{}{"key": key}

	if err := s.db.Send(&res, "query", query, params); err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}

	// Debug: Print the raw response structure
	jsonRes, _ := json.MarshalIndent(res, "", "  ")
	fmt.Printf("Response: %s\n", string(jsonRes))

	// Try to navigate through the response structure
	// This is a more flexible approach to handle different response formats
	resSlice, ok := res.([]interface{})
	if !ok || len(resSlice) == 0 {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	firstResult, ok := resSlice[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	resultItems, ok := firstResult["result"].([]interface{})
	if !ok || len(resultItems) == 0 {
		return nil, fmt.Errorf("no results found for key: %s", key)
	}

	item, ok := resultItems[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid item format")
	}

	// Check if entry is expired
	if e, exists := item["e"].(float64); exists && e != 0 {
		expiry := int64(e)
		if expiry <= time.Now().Unix() {
			// Entry is expired, delete it
			_ = s.Delete(key)
			return nil, fmt.Errorf("key expired: %s", key)
		}
	}

	if val, exists := item["v"].(string); exists {
		return []byte(val), nil
	}

	return nil, fmt.Errorf("value not found for key: %s", key)
}

// Set key with value
func (s *Storage) Set(key string, val []byte, exp time.Duration) error {
	var expiry int64 = 0
	if exp > 0 {
		expiry = time.Now().Add(exp).Unix()
	}

	params := map[string]interface{}{
		"key":    key,
		"value":  string(val),
		"expiry": expiry,
	}

	var res QueryResponse
	if err := s.db.Send(&res, "query", s.surrealInsert, params); err != nil {
		return fmt.Errorf("failed to set value: %w", err)
	}

	if res.Error != nil {
		return fmt.Errorf("database error: %s", res.Error.Message)
	}

	return nil
}

// Delete key
func (s *Storage) Delete(key string) error {
	var res QueryResponse
	params := map[string]interface{}{"key": key}

	if err := s.db.Send(&res, "query", s.surrealDelete, params); err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}

	if res.Error != nil {
		return fmt.Errorf("database error: %s", res.Error.Message)
	}

	return nil
}

// Reset all keys
func (s *Storage) Reset() error {
	var res QueryResponse
	if err := s.db.Send(&res, "query", s.surrealReset, nil); err != nil {
		return fmt.Errorf("failed to reset storage: %w", err)
	}

	if res.Error != nil {
		return fmt.Errorf("database error: %s", res.Error.Message)
	}

	return nil
}

// Close the database connection
func (s *Storage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Conn returns the SurrealDB client
func (s *Storage) Conn() *surrealdb.DB {
	return s.db
}

// Garbage collector to delete expired keys
func (s *Storage) gc() {
	ticker := time.NewTicker(s.config.GCInterval)
	defer ticker.Stop()

	for range ticker.C {
		var res QueryResponse
		params := map[string]interface{}{
			"expiry": time.Now().Unix(),
		}

		if err := s.db.Send(&res, "query", s.sqlGC, params); err != nil {
			// Just log the error and continue
			fmt.Printf("SurrealDB GC error: %v\n", err)
		}
	}
}

// GetAll retrieves all keys and values from storage
func (s *Storage) GetAll() (map[string][]byte, error) {
	query := fmt.Sprintf("SELECT * FROM %s", s.config.Table)
	var res QueryResponse

	if err := s.db.Send(&res, "query", query, nil); err != nil {
		return nil, fmt.Errorf("failed to get all: %w", err)
	}

	if res.Result == nil || len(*res.Result) == 0 {
		return make(map[string][]byte), nil
	}

	fmt.Println("1:", res.Result)

	result := (*res.Result)[0].Result
	entries, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	fmt.Println("entries:", entries)

	data := make(map[string][]byte)
	for _, entry := range entries {
		entryMap, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}

		key, keyExists := entryMap["k"].(string)
		val, valExists := entryMap["v"].(string)

		if keyExists && valExists {
			// Check if entry is expired
			if e, exists := entryMap["e"].(float64); exists && e != 0 {
				expiry := int64(e)
				if expiry <= time.Now().Unix() {
					continue // Skip expired entries
				}
			}

			data[key] = []byte(val)
		}
	}

	return data, nil
}

// Has checks if a key exists in the storage
func (s *Storage) Has(key string) (bool, error) {
	val, err := s.Get(key)
	if err != nil {
		// If error is specifically about key not found, return false, nil
		if err.Error() == fmt.Sprintf("key not found: %s", key) ||
			err.Error() == fmt.Sprintf("key expired: %s", key) {
			return false, nil
		}
		return false, err
	}
	return val != nil, nil
}

// Count returns the number of entries in the storage
func (s *Storage) Count() (int, error) {
	query := fmt.Sprintf("SELECT count() FROM %s", s.config.Table)
	var res QueryResponse

	if err := s.db.Send(&res, "query", query, nil); err != nil {
		return 0, fmt.Errorf("failed to count entries: %w", err)
	}

	if res.Result == nil || len(*res.Result) == 0 {
		return 0, nil
	}

	result := (*res.Result)[0].Result
	entries, ok := result.([]interface{})
	if !ok || len(entries) == 0 {
		return 0, fmt.Errorf("invalid response format")
	}

	countMap, ok := entries[0].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid count format")
	}

	if count, exists := countMap["count"].(float64); exists {
		return int(count), nil
	}

	return 0, fmt.Errorf("count not found in response")
}

// SetTTL updates the expiration time for an existing key
func (s *Storage) SetTTL(key string, exp time.Duration) error {
	// First check if the key exists
	_, err := s.Get(key)
	if err != nil {
		return err
	}

	var expiry int64 = 0
	if exp > 0 {
		expiry = time.Now().Add(exp).Unix()
	}

	query := fmt.Sprintf("UPDATE %s SET e = $expiry WHERE k = $key", s.config.Table)
	params := map[string]interface{}{
		"key":    key,
		"expiry": expiry,
	}

	var res QueryResponse
	if err := s.db.Send(&res, "query", query, params); err != nil {
		return fmt.Errorf("failed to set TTL: %w", err)
	}

	if res.Error != nil {
		return fmt.Errorf("database error: %s", res.Error.Message)
	}

	return nil
}

// GetTTL returns the remaining time until expiration
func (s *Storage) GetTTL(key string) (time.Duration, error) {
	query := fmt.Sprintf("SELECT e FROM %s WHERE k = $key", s.config.Table)
	params := map[string]interface{}{"key": key}

	var res QueryResponse
	if err := s.db.Send(&res, "query", query, params); err != nil {
		return 0, fmt.Errorf("failed to get TTL: %w", err)
	}

	if res.Result == nil || len(*res.Result) == 0 {
		return 0, fmt.Errorf("key not found: %s", key)
	}

	result := (*res.Result)[0].Result
	entries, ok := result.([]interface{})
	if !ok || len(entries) == 0 {
		return 0, fmt.Errorf("invalid response format")
	}

	entryMap, ok := entries[0].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid entry format")
	}

	if e, exists := entryMap["e"].(float64); exists {
		expiry := int64(e)
		if expiry == 0 {
			return 0, nil // No expiration
		}

		remaining := time.Until(time.Unix(expiry, 0))
		if remaining < 0 {
			return 0, fmt.Errorf("key expired: %s", key)
		}

		return remaining, nil
	}

	return 0, fmt.Errorf("expiry not found for key: %s", key)
}
