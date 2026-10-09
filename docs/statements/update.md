# UPDATE

`UPDATE` changes the values of the specified columns in all rows that satisfy the condition. Reference
documentation:
[PostgreSQL](https://www.postgresql.org/docs/current/sql-update.html),
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/update.html),
[MariaDB](https://mariadb.com/kb/en/library/update/),
[SQLite](https://www.sqlite.org/lang_update.html).

## Supported clauses

- `UPDATE(columns...)` - list of columns to update.
- `SET(values...)` - list of values for the columns.
- `MODEL(model)` - values are extracted from a model object.
- `WHERE(condition)` - only rows for which the condition is true are updated.
- `FROM(tables...)` - table expression allowing columns from other tables to appear in the `WHERE` condition
  and update expressions (PostgreSQL, SQLite).
- `RETURNING(output_expression...)` - expressions to compute and return after each row is updated
  (PostgreSQL, SQLite).

SQL table used in the examples:

```sql
CREATE TABLE IF NOT EXISTS link (
    id serial PRIMARY KEY,
    url VARCHAR (255) NOT NULL,
    name VARCHAR (255) NOT NULL,
    description VARCHAR (255)
);
```

## Update using SET

::: warning
The positional `SET("Yahoo", "http://yahoo.com")` form is not recommended, see `MODEL` below. The typed
`Link.Name.SET(String("Yahoo"))` form is fine.
:::

```go
// replace all Bing links with Yahoo
updateStmt := Link.UPDATE(Link.Name, Link.URL).
    SET("Yahoo", "http://yahoo.com").
    WHERE(Link.Name.EQ(String("Bing")))

// OR using type-safe SET
updateStmt := Link.UPDATE().
    SET(
        Link.Name.SET(String("Yahoo")),
        Link.URL.SET(String("http://yahoo.com")),
    ).
    WHERE(Link.Name.EQ(String("Bing")))
```

Debug SQL:

```sql
UPDATE test_sample.link          -- 'test_sample' is the schema name
SET (name, url) = ('Yahoo', 'http://yahoo.com')
WHERE link.name = 'Bing';
```

## Update using MODEL (recommended)

Model types add type and pointer safety to the update:

```go
yahoo := model.Link{
    URL:  "http://www.yahoo.com",
    Name: "Yahoo",
}

updateStmt := Link.
    UPDATE(Link.Name, Link.URL, Link.Description).
    MODEL(yahoo).
    WHERE(Link.Name.EQ(String("Bing")))
```

`Link.Name, Link.URL, Link.Description` can be replaced with `Link.MutableColumns` (all columns minus primary
key and generated columns). Primary key columns are usually not updated.

```go
updateStmt := Link.
    UPDATE(Link.MutableColumns).
    MODEL(yahoo).
    WHERE(Link.Name.EQ(String("Bing")))
```

A `ColumnList` can be used to pass a custom list of columns:

```go
columnList := ColumnList{Link.Name, Link.Description}
updateStmt := Link.
    UPDATE(columnList).
    MODEL(yahoo).
    WHERE(Link.Name.EQ(String("Bing")))
```

## UPDATE with JOIN

::: code-group

```go [PostgreSQL / CockroachDB / SQLite]
stmt := Staff.UPDATE(Staff.LastName).
	SET(String("Paul")).
	FROM(Address).
	WHERE(AND(
		Address.AddressID.EQ(Staff.AddressID),
		Staff.StaffID.EQ(Int(1)),
		Address.City.NOT_EQ(String("London")),
	))
```

```go [MySQL / MariaDB]
stmt := Staff.INNER_JOIN(Address, Address.AddressID.EQ(Staff.AddressID)).
	UPDATE(Staff.LastName).
	SET(String("Paul")).
	WHERE(Staff.StaffID.EQ(Int(1)).AND(Address.City.NOT_EQ(String("London"))))
```

:::

## Execute statement

To execute an update statement and get `sql.Result`:

```go
res, err := updateStmt.Exec(db)
```

To return the updated rows (PostgreSQL, SQLite), the statement needs a `RETURNING` clause:

```go
updateStmt := Link.
    UPDATE(Link.MutableColumns).
    MODEL(yahoo).
    WHERE(Link.Name.EQ(String("Bing"))).
    RETURNING(Link.AllColumns)

dest := []model.Link{}

err := updateStmt.Query(db, &dest)
```

Use `ExecContext` and `QueryContext` to pass a context.
