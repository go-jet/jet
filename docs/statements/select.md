# SELECT

`SELECT` retrieves rows from one or more tables. Reference documentation:
[PostgreSQL](https://www.postgresql.org/docs/current/sql-select.html),
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/select.html),
[MariaDB](https://mariadb.com/kb/en/library/select/),
[SQLite](https://www.sqlite.org/lang_select.html).

## Supported clauses

- `SELECT(expressions...)` - expressions forming the output rows.
- `OPTIMIZER_HINTS(hints...)` - per-statement query optimizer hints (MySQL only).
- `DISTINCT()` - remove duplicate rows from the result set.
- `FROM(tableSource...)` - one or more source tables.
- `WHERE(condition)` - only rows for which the condition is true are selected.
- `GROUP_BY(groupingElement...)` - group rows that share the same values of the grouped expressions into a
  single row.
- `HAVING(condition)` - eliminate group rows that do not satisfy the condition.
- `WINDOW(name)` - start a named window definition.
- `ORDER_BY(orderBy...)` - sort result rows by the given expressions.
- `LIMIT(count)` - maximum number of rows to return.
- `OFFSET(start)` - number of rows to skip before returning rows.
- `FOR(lock, additionalLocks...)` - row locking, see [FOR clause](#for-clause).
- `UNION(select)` / `UNION_ALL(select)` - set union of the involved SELECT statements.
- `INTERSECT(select)` / `INTERSECT_ALL(select)` - set intersection.
- `EXCEPT(select)` / `EXCEPT_ALL(select)` - rows in the left SELECT that are not in the right one.

_This list may be extended in future Jet releases._

## Example per clause

### SELECT clause

```go
// dot "." import implied
SELECT(
    Int(1).ADD(Int(12)).SUB(Int(21)),                           // arbitrary expression
    Film.Name,                                                  // column
    Customer.FirstName.CONCAT(Customer.LastName).AS("FullName"), // alias
)
```

Generated SQL:

```sql
SELECT 1 + 12 - 21,
       film.name AS "film.name",
       customer.first_name || customer.last_name AS "FullName"
```

`film.name AS "film.name"` - column names are aliased by default. The alias is used during execution to map
the result row into the matching `model` struct.

### OPTIMIZER_HINTS (MySQL only)

```go
SELECT(Actor.ActorID).
OPTIMIZER_HINTS(MAX_EXECUTION_TIME(1), QB_NAME("mainQueryBlock"), "NO_ICP(actor)")
```

```sql
SELECT /*+ MAX_EXECUTION_TIME(1) QB_NAME(mainQueryBlock) NO_ICP(actor) */ actor.actor_id AS "actor.actor_id"
```

### DISTINCT clause

```go
SELECT(Film.Name).
DISTINCT()

// PostgreSQL only: DISTINCT ON
SELECT(Film.Duration, Film.Rating, Film.Name).
DISTINCT(Film.Duration, Film.Rating)
```

```sql
SELECT DISTINCT film.name AS "film.name"

-- PostgreSQL only
SELECT DISTINCT ON (film.duration, film.rating)
     film.duration AS "film.duration",
     film.rating AS "film.rating",
     film.name AS "film.name"
```

### FROM clause

The FROM clause specifies one or more source tables.

```go
// 1) single table
.FROM(Film)

// 2) join
.FROM(
    Film.
        INNER_JOIN(Language, Language.LanguageID.EQ(Film.LanguageID)),
)

// 3) implicit CROSS JOIN
.FROM(Film, Language, Actor)
```

```sql
-- 1)
FROM dvds.film

-- 2)
FROM dvds.film
     INNER JOIN dvds.language ON (language.language_id = film.language_id)

-- 3)
FROM dvds.film, dvds.language, dvds.actor
```

Supported joins: `INNER_JOIN`, `LEFT_JOIN`, `RIGHT_JOIN`, `FULL_JOIN`, `CROSS_JOIN`. Table sources can be
tables, views, [sub-queries](./subquery), [CTEs](./with), [VALUES](./values) and `LATERAL` sub-queries.

### WHERE clause

```go
.WHERE(Film.Length.GT(Int(150)))
```

```sql
WHERE film.length > 150
```

### GROUP BY clause

```go
// 1)
.GROUP_BY(Film.Length)

// 2) PostgreSQL
.GROUP_BY(
	GROUPING_SETS(
		WRAP(Inventory.FilmID, Inventory.StoreID),
		WRAP(Inventory.FilmID),
		WRAP(),
	),
)

// 3) PostgreSQL
.GROUP_BY(
	CUBE(Country.Country, City.City),
)

// 4) PostgreSQL
.GROUP_BY(
	ROLLUP(Country.Country, City.City),
)

// 5) MySQL
.GROUP_BY(
	WITH_ROLLUP(Inventory.FilmID, Inventory.StoreID),
)
```

```sql
-- 1)
GROUP BY film.length
-- 2)
GROUP BY GROUPING SETS((inventory.film_id, inventory.store_id), (inventory.film_id), ())
-- 3)
GROUP BY CUBE(country.country, city.city)
-- 4)
GROUP BY ROLLUP(country.country, city.city)
-- 5)
GROUP BY inventory.film_id, inventory.store_id WITH ROLLUP
```

### WINDOW clause

```go
SELECT(
    AVG(Payment.Amount).OVER(),
    MINf(Payment.Amount).OVER(PARTITION_BY(Payment.CustomerID).ORDER_BY(Payment.PaymentDate.DESC())),
    ROW_NUMBER().OVER(Window("w1")),
    RANK().OVER(
       Window("w2").
       ORDER_BY(Payment.CustomerID).
       RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED)),
    ),
    AVG(Payment.Amount).OVER(Window("w3").ROWS(PRECEDING(1), FOLLOWING(2))),
).
FROM(Payment).
WINDOW("w1").AS(PARTITION_BY(Payment.PaymentDate)).
WINDOW("w2").AS(Window("w1")).
WINDOW("w3").AS(Window("w2").ORDER_BY(Payment.CustomerID))
```

```sql
SELECT AVG(payment.amount) OVER (),
     MIN(payment.amount) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC),
     ROW_NUMBER() OVER (w1),
     RANK() OVER (w2 ORDER BY payment.customer_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
     AVG(payment.amount) OVER (w3 ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)
FROM dvds.payment
WINDOW w1 AS (PARTITION BY payment.payment_date), w2 AS (w1), w3 AS (w2 ORDER BY payment.customer_id);
```

### HAVING clause

```go
.HAVING(SUMi(Film.Length).GT(Int(150)))
```

```sql
HAVING SUM(film.length) > 150
```

### ORDER BY clause

```go
.ORDER_BY(Film.Length)                       // default direction
.ORDER_BY(Film.Length.DESC(), Film.Title.ASC())
```

```sql
ORDER BY film.length
ORDER BY film.length DESC, film.title ASC
```

### LIMIT and OFFSET clauses

```go
.LIMIT(11).OFFSET(22)
```

```sql
LIMIT 11
OFFSET 22
```

### FOR clause

`FOR` controls how `SELECT` locks rows as they are read. Each lock accepts optional `OF(tables...)` to restrict
the lock to specific tables, and optional `NOWAIT()` or `SKIP_LOCKED()`.

Available lock strengths:

| Dialect    | Lock strengths                                            |
| ---------- | --------------------------------------------------------- |
| PostgreSQL | `UPDATE()`, `NO_KEY_UPDATE()`, `SHARE()`, `KEY_SHARE()`   |
| MySQL      | `UPDATE()`, `SHARE()`; also `LOCK_IN_SHARE_MODE()` clause |
| MariaDB    | `UPDATE()`; also `LOCK_IN_SHARE_MODE()` clause            |

```go
.FOR(NO_KEY_UPDATE().SKIP_LOCKED())
```

```sql
FOR NO KEY UPDATE SKIP LOCKED
```

Lock only specific tables in a join:

```go
.FOR(UPDATE().OF(Film, Actor).NOWAIT())
```

```sql
FOR UPDATE OF film, actor NOWAIT
```

Multiple locking clauses can be combined to lock different tables with different strengths. Each clause must
name its tables with `OF`:

```go
myActor := Actor.AS("myActor")

SELECT(
    Film.FilmID,
    FilmActor.ActorID,
    myActor.FirstName,
    FilmCategory.CategoryID,
).FROM(
    Film.
        INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(Film.FilmID)).
        INNER_JOIN(myActor, myActor.ActorID.EQ(FilmActor.ActorID)).
        INNER_JOIN(FilmCategory, FilmCategory.FilmID.EQ(Film.FilmID)),
).FOR(
    SHARE().OF(Film, FilmActor),
    UPDATE().OF(myActor).NOWAIT(),
)
```

```sql
...
FOR SHARE OF film, film_actor
FOR UPDATE OF "myActor" NOWAIT
```

This takes a shared lock on the `film` and `film_actor` rows, an exclusive lock on the `actor` rows, and no row
lock on `film_category`.

::: warning Dialect differences
- Multiple locking clauses and `OF` are supported by PostgreSQL and MySQL 8.0.1+.
- MySQL requires every clause to use `OF` when more than one is present, and a table may appear in only one
  clause. Aliased tables must be referenced by their alias.
- MariaDB supports only a single `FOR UPDATE` / `LOCK IN SHARE MODE` without `OF`.
- SQLite has no row-level locking syntax.

Jet does not validate these rules; the database returns a syntax error at execution time.
:::

### Set clauses (UNION, UNION_ALL, INTERSECT, INTERSECT_ALL, EXCEPT, EXCEPT_ALL)

```go
SELECT(Payment.Amount).FROM(Payment).
UNION_ALL(SELECT(Payment.Amount).FROM(Payment))
```

```sql
(
     SELECT payment.amount AS "payment.amount"
     FROM dvds.payment
)
UNION ALL
(
     SELECT payment.amount AS "payment.amount"
     FROM dvds.payment
);
```

## Two forms of select statements

### Classical select statement

Selected columns come before the table sources (`FROM` clause):

```go
SELECT(
    Payment.AllColumns,
    Customer.AllColumns,
).FROM(
    Payment.
        INNER_JOIN(Customer, Payment.CustomerID.EQ(Customer.CustomerID)),
).ORDER_BY(
    Payment.PaymentID.ASC(),
).LIMIT(30)
```

### Jet select statement

Table sources come before the selected columns. There is no `FROM` clause:

```go
Payment.
    INNER_JOIN(Customer, Payment.CustomerID.EQ(Customer.CustomerID)).
    SELECT(
        Payment.AllColumns,
        Customer.AllColumns,
    ).ORDER_BY(
        Payment.PaymentID.ASC(),
    ).LIMIT(30)
```

::: tip
The Jet form exists because it is sometimes more natural to think about the tables first and the columns
second. The classical form is still preferred because it reads more like native SQL.
**Both forms produce exactly the same SQL.**
:::

## Table aliasing

```go
// alias first
manager := Employee.AS("Manager")

// then use the aliased table in a statement
stmt := SELECT(
	manager.AllColumns,
).FROM(
	manager,
)
```

```sql
SELECT "Manager"."EmployeeId" AS "Manager.EmployeeId",
     "Manager"."LastName" AS "Manager.LastName",
     "Manager"."FirstName" AS "Manager.FirstName",
     "Manager"."Title" AS "Manager.Title"
FROM chinook."Employee" AS "Manager";
```

`model.Employee` can no longer be used directly as the destination for this query, because the expected
destination type name is now `Manager`. There are two options:

1. Define a new type:

   ```go
   type Manager model.Employee
   var dest Manager

   err := stmt.Query(db, &dest)
   ```

2. Use field aliasing:

   ```go
   var dest struct {
       Manager model.Employee `alias:"Manager.*"`
   }

   err := stmt.Query(db, &dest)
   ```

See also [Sub-query](./subquery) for using a `SELECT` as a table source.
