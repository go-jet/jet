# INSERT

`INSERT` inserts one or more rows into a table. Reference documentation:
[PostgreSQL](https://www.postgresql.org/docs/current/sql-insert.html),
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/insert.html),
[MariaDB](https://mariadb.com/kb/en/library/insert/),
[SQLite](https://www.sqlite.org/lang_insert.html).

## Supported clauses

- `INSERT(columns...)` - list of columns to insert.
- `VALUES(values...)` - list of values.
- `MODEL(model)` - values are extracted from a model object.
- `MODELS([]model)` - values are extracted from a slice of model objects.
- `QUERY(select)` - a select statement supplying the rows to insert.
- `ON_CONFLICT(...)` - alternative action on unique or exclusion constraint violation (PostgreSQL, SQLite).
- `ON_DUPLICATE_KEY_UPDATE(...)` - update existing rows if the insert would cause a duplicate value in a
  `UNIQUE` index or `PRIMARY KEY` (MySQL, MariaDB).
- `RETURNING(output_expression...)` - expressions to compute and return after each row is inserted. Use
  `TableName.AllColumns` to return all columns (PostgreSQL, SQLite, MariaDB).

_This list may be extended in future Jet releases._

SQL table used in the examples:

```sql
CREATE TABLE IF NOT EXISTS link (
    id serial PRIMARY KEY,
    url VARCHAR (255) NOT NULL,
    name VARCHAR (255) NOT NULL,
    description VARCHAR (255)
);
```

## Insert row by row

### Using VALUES

::: warning
Not recommended, see `MODEL` / `MODELS` below for a type-safe alternative.
:::

```go
insertStmt := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
    VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
    VALUES(101, "http://www.google.com", "Google", DEFAULT).
    VALUES(102, "http://www.yahoo.com", "Yahoo", nil)
```

Debug SQL:

```sql
INSERT INTO test_sample.link (id, url, name, description) VALUES
     (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
     (101, 'http://www.google.com', 'Google', DEFAULT),
     (102, 'http://www.yahoo.com', 'Yahoo', NULL)
```

### Using MODEL and MODELS (recommended)

Model types add type and pointer safety to the insert:

```go
tutorial := model.Link{
    ID:   100,
    URL:  "http://www.postgresqltutorial.com",
    Name: "PostgreSQL Tutorial",
}

google := model.Link{
    ID:   101,
    URL:  "http://www.google.com",
    Name: "Google",
}

yahoo := model.Link{
    ID:   102,
    URL:  "http://www.yahoo.com",
    Name: "Yahoo",
}

insertStmt := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
    MODEL(tutorial).
    MODEL(google).
    MODEL(yahoo)
```

Or shorter, if the model data is in a slice:

```go
insertStmt := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
    MODELS([]model.Link{tutorial, google, yahoo})
```

`Link.ID, Link.URL, Link.Name, Link.Description` is the same as `Link.AllColumns`, so the statement can be
simplified to:

```go
insertStmt := Link.INSERT(Link.AllColumns).
    MODELS([]model.Link{tutorial, google, yahoo})
```

`Link.ID` is an auto-increment primary key column, so it can be omitted from the insert. `Link.MutableColumns`
is a shorthand for all columns minus primary key and generated columns:

```go
insertStmt := Link.INSERT(Link.MutableColumns).
    MODELS([]model.Link{tutorial, google, yahoo})
```

A `ColumnList` can be used to pass a custom list of columns:

```go
columnList := ColumnList{Link.Name, Link.Description}
insertStmt := Link.INSERT(columnList).
    MODEL(tutorial)
```

`VALUES`, `MODEL` and `MODELS` can be mixed in the same statement:

```go
insertStmt := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
    VALUES(101, "http://www.google.com", "Google", DEFAULT).
    MODEL(tutorial).
    MODELS([]model.Link{yahoo})
```

## Insert using a query

```go
// duplicate the first 10 entries
insertStmt := Link.
    INSERT(Link.URL, Link.Name).
    QUERY(
        SELECT(Link.URL, Link.Name).
            FROM(Link).
            WHERE(Link.ID.GT(Int(0)).AND(Link.ID.LT_EQ(Int(10)))),
    )
```

## Upsert

### PostgreSQL and SQLite: ON CONFLICT

`ON CONFLICT DO NOTHING`:

```go
Employee.INSERT(Employee.AllColumns).
    MODEL(employee).
    ON_CONFLICT(Employee.EmployeeID).DO_NOTHING()
```

`ON CONFLICT DO UPDATE`:

```go
Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
    VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
    ON_CONFLICT(Link.ID).DO_UPDATE(
        SET(
            Link.ID.SET(Link.EXCLUDED.ID),
            Link.URL.SET(String("http://www.postgresqltutorial2.com")),
        ),
    )
```

`ON CONFLICT ... WHERE ... DO UPDATE ... WHERE`:

```go
Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
    VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
    ON_CONFLICT(Link.ID).
        WHERE(Link.ID.MUL(Int(2)).GT(Int(10))).
        DO_UPDATE(
            SET(
                Link.ID.SET(
                    IntExp(SELECT(MAXi(Link.ID).ADD(Int(1))).
                        FROM(Link)),
                ),
                ColumnList{Link.Name, Link.Description}.SET(ROW(Link.EXCLUDED.Name, String("new description"))),
            ).WHERE(Link.Description.IS_NOT_NULL()),
        )
```

### MySQL and MariaDB: ON DUPLICATE KEY UPDATE

```go
Link.INSERT().
	VALUES(randID, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
	ON_DUPLICATE_KEY_UPDATE(
		Link.ID.SET(Link.ID.ADD(Int(11))),
		Link.Name.SET(String("PostgreSQL Tutorial 2")),
	)
```

Referencing the new row values via `AS_NEW()` and the `NEW` alias:

```go
Link.INSERT().
	MODEL(model.Link{
		ID:          randID,
		URL:         "https://www.postgresqltutorial.com",
		Name:        "PostgreSQL Tutorial",
		Description: nil,
	}).AS_NEW(). // Note !!!
	ON_DUPLICATE_KEY_UPDATE(
		Link.URL.SET(Link.NEW.URL),
		Link.Name.SET(Link.NEW.Name),
	)
```

## Execute statement

To execute an insert statement and get `sql.Result`:

```go
res, err := insertStmt.Exec(db)
```

To return the inserted records, the statement needs a `RETURNING` clause:

```go
insertStmt := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
    VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
    VALUES(101, "http://www.google.com", "Google", DEFAULT).
    RETURNING(Link.ID, Link.URL, Link.Name, Link.Description) // or RETURNING(Link.AllColumns)

dest := []model.Link{}

err := insertStmt.Query(db, &dest)
```

Use `ExecContext` and `QueryContext` to pass a context.
