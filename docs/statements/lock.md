# LOCK

`LOCK` obtains a table-level lock, waiting if necessary for any conflicting locks to be released. Reference
documentation:
[PostgreSQL](https://www.postgresql.org/docs/current/sql-lock.html),
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html),
[MariaDB](https://mariadb.com/kb/en/library/lock-tables/).

::: tip Row-level locks
For row-level locks (`SELECT ... FOR UPDATE`, `FOR SHARE`) see the [FOR clause](./select#for-clause) of the
SELECT statement.
:::

## PostgreSQL

Supported clauses:

- `IN(mode)` - specifies which locks this lock conflicts with. Mode can be:
  - `LOCK_ACCESS_SHARE`
  - `LOCK_ROW_SHARE`
  - `LOCK_ROW_EXCLUSIVE`
  - `LOCK_SHARE_UPDATE_EXCLUSIVE`
  - `LOCK_SHARE`
  - `LOCK_SHARE_ROW_EXCLUSIVE`
  - `LOCK_EXCLUSIVE`
  - `LOCK_ACCESS_EXCLUSIVE`
- `NOWAIT()` - do not wait for conflicting locks to be released. If the lock cannot be acquired immediately,
  the transaction is aborted.

```go
lockStmt := Address.
        LOCK().
        IN(LOCK_ACCESS_SHARE).
        NOWAIT()
```

Debug SQL:

```sql
LOCK TABLE dvds.address IN ACCESS SHARE MODE NOWAIT;
```

## MySQL and MariaDB

MySQL locks tables with `LOCK TABLES ... READ | WRITE` and releases them with `UNLOCK TABLES`:

```go
lockStmt := Customer.LOCK().READ()   // or .WRITE()
unlockStmt := UNLOCK_TABLES()
```

```sql
LOCK TABLES dvds.customer READ;
UNLOCK TABLES;
```

## Execute statement

To execute a lock statement and get `sql.Result`:

```go
res, err := lockStmt.Exec(db)
```

Use `ExecContext` to pass a context. Lock statements only make sense inside a transaction, so `db` is
usually a `*sql.Tx`.
