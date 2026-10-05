# DELETE

`DELETE` deletes rows that satisfy the `WHERE` clause from the specified table. Reference documentation:
[PostgreSQL](https://www.postgresql.org/docs/current/sql-delete.html),
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/delete.html),
[MariaDB](https://mariadb.com/kb/en/library/delete/),
[SQLite](https://www.sqlite.org/lang_delete.html).

## Supported clauses

- `USING(tables...)` - allows columns from other tables to appear in the `WHERE` condition (PostgreSQL, MySQL).
- `WHERE(condition)` - only rows for which the condition is true are deleted.
- `ORDER_BY(...)` - rows are deleted in the specified order (MySQL, MariaDB, SQLite).
- `LIMIT(count)` - maximum number of rows to delete (MySQL, MariaDB, SQLite).
- `RETURNING(output_expression...)` - expressions to compute and return after each row is deleted. Use
  `TableName.AllColumns` to return all columns (PostgreSQL, SQLite, MariaDB).

SQL table used in the examples:

```sql
CREATE TABLE IF NOT EXISTS link (
    id serial PRIMARY KEY,
    url VARCHAR (255) NOT NULL,
    name VARCHAR (255) NOT NULL,
    description VARCHAR (255)
);
```

## Example

```go
// delete all links named 'Gmail' or 'Outlook'
deleteStmt := Link.
    DELETE().
    WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))
```

Debug SQL:

```sql
DELETE FROM test_sample.link      -- 'test_sample' is the schema name
WHERE link.name IN ('Gmail', 'Outlook');
```

## Execute statement

To execute a delete statement and get `sql.Result`:

```go
res, err := deleteStmt.Exec(db)
```

To return the deleted rows, the statement needs a `RETURNING` clause:

```go
deleteStmt := Link.
    DELETE().
    WHERE(Link.Name.IN(String("Gmail"), String("Outlook"))).
    RETURNING(Link.AllColumns)

dest := []model.Link{}

err := deleteStmt.Query(db, &dest)
```

Use `ExecContext` and `QueryContext` to pass a context.
