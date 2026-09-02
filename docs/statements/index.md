# Statements

## Executing statements

The following statements are supported:

- [SELECT](./select) and [SELECT_JSON](./select-json)
- [INSERT](./insert)
- [UPDATE](./update)
- [DELETE](./delete)
- [LOCK](./lock)
- [WITH](./with)
- [VALUES](./values)

Statements can be executed with the following methods:

- `Query(db qrm.Queryable, dest interface{}) error` - executes the statement over database connection or
  transaction `db` and stores the result in `dest`. The destination can be a pointer to a struct or a pointer
  to a slice. If the destination is a pointer to a struct and the result set is empty, `qrm.ErrNoRows` is
  returned.
- `QueryContext(ctx context.Context, db qrm.Queryable, dest interface{}) error` - same as `Query`, with a
  context.
- `Exec(db qrm.Executable) (sql.Result, error)` - executes the statement over `db` and returns `sql.Result`.
- `ExecContext(ctx context.Context, db qrm.Executable) (sql.Result, error)` - same as `Exec`, with a context.
- `Rows(ctx context.Context, db qrm.Queryable) (*Rows, error)` - executes the statement over `db` and returns
  rows for manual iteration.

Each execution method builds a parameterized SQL query with its arguments, then calls the matching method on
`db`.

`Exec` and `ExecContext` are thin wrappers around the `Exec` and `ExecContext` methods of `db`.

`Query` and `QueryContext` are Query Result Mapping methods. They execute the statement with `QueryContext`
on `db` and group each row of the result into the `destination`. See
[Query Result Mapping (QRM)](../guide/qrm).

`db` can be any type implementing the following interfaces:

```go
type Queryable interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type Executable interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
```

These include, but are not limited to, `*sql.DB`, `*sql.Tx` and `*sql.Conn`.

## Debugging statements

The SQL generated from a statement can be inspected with:

- `Sql() (query string, args []interface{})` - the parameterized SQL query with its list of arguments.
- `DebugSql() (query string)` - a debug query where every placeholder is replaced with the textual
  representation of its argument.

::: warning
`DebugSql` is for debugging only. Never execute its output in production, as it is vulnerable to SQL injection.
:::

## Logging statements

Statement execution information can be captured for logging or metrics by providing a global query logger:

```go
postgres.SetQueryLogger(func(ctx context.Context, queryInfo postgres.QueryInfo) {
	sql, args := queryInfo.Statement.Sql()
	fmt.Printf("- SQL: %s Args: %v \n", sql, args)
	fmt.Printf("- Debug SQL: %s \n", queryInfo.Statement.DebugSql())

	// Depending on how the statement is executed, RowsProcessed is:
	//   - Number of rows returned for Query() and QueryContext() methods
	//   - RowsAffected() for Exec() and ExecContext() methods
	//   - Always 0 for Rows() method.
	fmt.Printf("- Rows processed: %d\n", queryInfo.RowsProcessed)
	fmt.Printf("- Duration %s\n", queryInfo.Duration.String())
	fmt.Printf("- Execution error: %v\n", queryInfo.Err)

	callerFile, callerLine, callerFunction := queryInfo.Caller()
	fmt.Printf("- Caller file: %s, line: %d, function: %s\n", callerFile, callerLine, callerFunction)
})
```

The same function exists in the `mysql` and `sqlite` packages.

## Raw statements

It is possible to write raw SQL queries without the generated SQL builder types. With raw queries all the
benefits of type safety and code completion are lost. Every projection has to be aliased in the
`destination type name`.`field name` format, otherwise QRM cannot scan the result.

```go
stmt := RawStatement(`
	SELECT actor.actor_id AS "actor.actor_id",
		 actor.first_name AS "actor.first_name",
		 actor.last_name AS "actor.last_name",
		 actor.last_update AS "actor.last_update"
	FROM dvds.actor
	WHERE actor.actor_id IN (actorID1, #actorID2, $actorID3) AND ((actorID1 / #actorID2) <> (#actorID2 * $actorID3))
	ORDER BY actor.actor_id`,
	RawArgs{
		"actorID1":  int64(1),
		"#actorID2": int64(2),
		"$actorID3": int64(3),
	},
)

var actors []model.Actor
err := stmt.Query(db, &actors)
```

`RawArgs` contains named arguments for the placeholders in the raw query. The naming convention is free-form,
the names just have to match the placeholders exactly. _It is recommended NOT to use `$1, $2, ...` for
PostgreSQL queries._

## Examples

### Query with `QueryContext`

`QueryContext` maps a statement result into a single object or into a slice:

::: code-group

```go [Single object]
stmt := SELECT(
	Actor.AllColumns,
).FROM(
	Actor,
).WHERE(
	Actor.ActorID.EQ(Int(2)),
)

actor := model.Actor{}
err := stmt.QueryContext(ctx, db, &actor)
```

```go [Slice]
stmt := SELECT(
	Actor.AllColumns,
).FROM(
	Actor,
).WHERE(
	Actor.ActorID.GT(Int(20)),
)

actors := []model.Actor{}
err := stmt.QueryContext(ctx, db, &actors)
```

:::

### Execute with `ExecContext`

```go
linkData := model.Link{
	ID:   1000,
	URL:  "http://www.duckduckgo.com",
	Name: "Duck Duck go",
}

stmt := Link.
	INSERT().
	MODEL(linkData)

res, err := stmt.ExecContext(ctx, db)
```

### Iterate with `Rows`

```go
stmt := SELECT(
	Inventory.AllColumns,
	Film.AllColumns,
	Store.AllColumns,
).FROM(
	Inventory.
		INNER_JOIN(Film, Film.FilmID.EQ(Inventory.FilmID)).
		INNER_JOIN(Store, Store.StoreID.EQ(Inventory.StoreID)),
).ORDER_BY(
	Inventory.InventoryID.ASC(),
)

rows, err := stmt.Rows(ctx, db)
defer rows.Close()

for rows.Next() {
	var inventory struct {
		model.Inventory

		Film  model.Film
		Store model.Store
	}

	err = rows.Scan(&inventory)
	...
}

err = rows.Close()
...

err = rows.Err()
...
```

`Scan` relies on reflection to map each row into the destination. For very large result sets this can add
latency. To avoid reflection, access the underlying `sql.Rows` and scan directly into fields:

```go
for rows.Next() {
	var inventory model.Inventory

	err = rows.Rows.Scan(&inventory.InventoryID, &inventory.FilmID, &inventory.StoreID, &inventory.LastUpdate)
	...
}
```

## Prepared statement caching

The standard Go SQL library prepares a statement for each query before executing it. Each `Query` or `Exec`
therefore costs two database round trips: one to prepare and one to execute. With prepared statement caching,
a statement is prepared once and reused, halving the number of database calls for repeated queries.

To enable it, only the database initialization code changes. Instead of:

```go
var db *sql.DB

db, err := sql.Open(driverName, connectionString)
```

use:

```go
sqlDB, err := sql.Open(driverName, connectionString)

var db *stmtcache.DB

db = stmtcache.New(sqlDB)
```

`stmtcache.DB` wraps `sql.DB` and intercepts all database queries. For a new query it first creates a prepared
statement, caches it, then executes it. Subsequent identical queries reuse the cached prepared statement.

::: warning
Do not use prepared statement caching with raw expressions or statements that have hard-coded parameters. The
prepared statements will not be reused, and a new one is created and cached for every distinct parameter
value, which leaks memory.
:::

Since `stmtcache.DB` has the same interface as `sql.DB`, it can be used anywhere `sql.DB` is used:

```go
var db *stmtcache.DB
...
err := stmt.Query(db, &dest)
res, err := stmt.Exec(db)
tx := db.Begin() // caching is enabled on transactions created from *stmtcache.DB as well
```

### Skipping the cache

To bypass statement caching for a single call, use the underlying `sql.DB`:

```go
var db *stmtcache.DB
...
err := stmt.Query(db.DB, &dest)
```

### When to use prepared statement caching

Consider it when:

- **Geographical distance**: the application server and database are far apart (for example, different cloud
  regions), so saving a round trip matters.
- **Large queries**: complex or long queries benefit from not being re-parsed by the database each time.
- **Repetitive queries**: querying in a loop (for example, during a data migration).

If the application and database are close (for example, SQLite) and queries are simple, caching may add
latency, since each call requires a mutex lock and a map lookup. If a connection pooler such as PgBouncer is
used, prepared statement caching should be avoided or configured according to the pooler mode.
