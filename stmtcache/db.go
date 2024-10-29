package stmtcache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

// DB is a wrapper for sql.DB, providing an additional layer for caching prepared statements
// to optimize database interactions and improve performance.
type DB struct {
	*sql.DB

	cachingEnabled bool

	lock       sync.RWMutex
	statements map[string]*sql.Stmt
}

// New creates new DB wrapper with statements caching enabled
func New(db *sql.DB) *DB {
	return &DB{
		DB:             db,
		cachingEnabled: true,
		statements:     make(map[string]*sql.Stmt),
	}
}

// SetCaching returns *DB wrapper with prepared statements caching enabled or disabled. This method should be
// called only once. It is not concurrency-safe.
func (d *DB) SetCaching(enabled bool) *DB {
	d.cachingEnabled = enabled
	return d
}

// CachingEnabled returns true if statements caching is enabled
func (d *DB) CachingEnabled() bool {
	return d.cachingEnabled
}

// CacheSize returns the current number of prepared statements stored in the cache.
func (d *DB) CacheSize() int {
	d.lock.RLock()
	ret := len(d.statements)
	d.lock.RUnlock()
	return ret
}

// Begin starts a new SQL transaction and returns a Tx object with statement caching capabilities.
func (d *DB) Begin() (*Tx, error) {
	tx, err := d.DB.Begin()

	if err != nil {
		return nil, err
	}

	return &Tx{
		Tx:         tx,
		db:         d,
		statements: make(map[string]*sql.Stmt),
	}, nil
}

// BeginTx starts a new SQL transaction and returns a Tx object with statement caching capabilities.
func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := d.DB.BeginTx(ctx, opts)

	if err != nil {
		return nil, err
	}

	return &Tx{
		Tx:         tx,
		db:         d,
		statements: make(map[string]*sql.Stmt),
	}, nil
}

// Exec executes a query that doesn't return rows. Exec delegates call to ExecContext with contex.Background()
// as parameter.
func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return d.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query that doesn't return rows. If statement caching is enabled, ExecContext will
// first call PrepareContext to retrieve a prepared statement, and then execute a query using a prepared statement.
// If statement caching is disabled, this method delegates the call to the *sql.DB ExecContext method.
func (d *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if !d.cachingEnabled {
		return d.DB.ExecContext(ctx, query, args...)
	}

	prepStmt, err := d.PrepareContext(ctx, query)

	if err != nil {
		return nil, err
	}

	return prepStmt.ExecContext(ctx, args...)
}

// Query delegates call to QueryContext using context.Background() as parameter.
func (d *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return d.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows. If statement caching is enabled, QueryContext will
// first call PrepareContext to retrieve a prepared statement, and then execute a query using a prepared statement.
// If statement caching is disabled, this method delegates the call to the *sql.DB QueryContext method.
func (d *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if !d.cachingEnabled {
		return d.DB.QueryContext(ctx, query, args...)
	}

	prepStmt, err := d.PrepareContext(ctx, query)

	if err != nil {
		return nil, err
	}

	return prepStmt.QueryContext(ctx, args...)
}

// Prepare delegates call to PrepareContext using context.Background as a parameter.
func (d *DB) Prepare(query string) (*sql.Stmt, error) {
	return d.PrepareContext(context.Background(), query)
}

// PrepareContext returns database prepared statement for a query. When statement caching is enabled, it returns a cached
// prepared statement if available; otherwise, it creates a new prepared statement and adds it to the cache.
// Invoking this method directly is unnecessary, as wrapper methods like Exec/ExecContext and Query/QueryContext
// will call PrepareContext before executing a query on it.
// If statement caching is disabled, this method delegates the call to the *sql.DB PrepareContext method.
//
// There's no need to manually close the returned statement; it operates within the transaction scope and will be closed
// automatically upon the completion of the transaction, whether it's committed or rolled back.
func (d *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if !d.cachingEnabled {
		return d.DB.PrepareContext(ctx, query)
	}

	d.lock.RLock()
	prepStmt, ok := d.statements[query]
	d.lock.RUnlock()

	if ok {
		return prepStmt, nil
	}

	prepStmt, err := d.DB.PrepareContext(ctx, query)

	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement %s: %w", query, err)
	}

	d.lock.Lock()

	existingPrepStmt, exist := d.statements[query]

	// if in the meantime, another goroutine created prepared statements for this query, we will close this
	// prepared statement and return the existing one.
	if exist {
		_ = prepStmt.Close()
		d.lock.Unlock()
		return existingPrepStmt, nil
	}

	d.statements[query] = prepStmt
	d.lock.Unlock()
	return prepStmt, nil
}

// ClearCache will close all cached prepared statements and clear statements cache map
func (d *DB) ClearCache() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	var err error

	for _, statement := range d.statements {
		closeErr := statement.Close()

		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}

	d.statements = make(map[string]*sql.Stmt)

	if err != nil {
		return errors.Join(errors.New("jet: some of the prepared statements failed to close"), err)
	}

	return nil
}

// Close will clear the statements cache and close the underlying db connection
func (d *DB) Close() error {
	clearErr := d.ClearCache()
	closeErr := d.DB.Close()

	return errors.Join(clearErr, closeErr)
}
