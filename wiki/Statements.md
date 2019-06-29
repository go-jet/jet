
Following statements are supported: 

* [SELECT](https://github.com/go-jet/jet/wiki/SELECT)
* [INSERT](https://github.com/go-jet/jet/wiki/INSERT)
* [UPDATE](https://github.com/go-jet/jet/wiki/UPDATE)
* [DELETE](https://github.com/go-jet/jet/wiki/DELETE)
* [LOCK](https://github.com/go-jet/jet/wiki/LOCK)

_This list might be extended with feature Jet releases._ 

There is a common set of action that can be performed for each statement type:

- `Sql() (query string, args []interface{}, err error)` - retrieves parametrized sql query with list of arguments
- `DebugSql() (query string, err error)` - retrieves debug query where every parametrized placeholder is replaced with its argument.
- `Query(db execution.DB, destination interface{}) error` - executes statements over database connection db and stores row result in destination.
- `QueryContext(db execution.DB, context context.Context, destination interface{}) error` - executes statement with a context over database connection db and stores row result in destination.
- `Exec(db execution.DB) (sql.Result, error)` - executes statement over db connection without returning any rows.
- `ExecContext(db execution.DB, context context.Context) (sql.Result, error)` - executes statement with context over db connection without returning any rows.

Database connection can be of any type that implements following interface:

```go
type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}
```

These include but are not limited to: 
- `sql.DB`
- `sql.Tx`
- `sql.Conn`