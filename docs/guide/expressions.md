# Expressions

The Jet SQL builder supports the following expression types:

- Bool expressions
- Integer expressions
- Float expressions
- String expressions
- Blob/Bytea expressions
- Date expressions
- Time expressions
- Timez expressions (time with time zone)
- Timestamp expressions
- Timestampz expressions (timestamp with time zone)
- Interval expressions
- Range expressions
- Row (tuple) expressions
- Raw expressions

::: info
This list may be extended in future Jet releases. Not every SQL dialect supports every expression type.
:::

## Literal types

For every expression type there is a constructor for a literal value:

```go
Bool(true)
Int(11), UInt(22), Int64(-10000), UInt64(200000), ...
Float(23.44), Decimal("11.20000345")
String("John Doe"), UUID(uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
Date(2010, 12, 3)
Time(23, 6, 6, 1)
Timez(23, 6, 6, 222, +200)
Timestamp(2010, 10, 21, 15, 30, 12, 333)
Timestampz(2010, 10, 21, 15, 30, 12, 444, 0)
Bytea("byte array"), Bytea([]byte("byte array"))
Json(`{"firstName": "John", "lastName": "Doe"}`)
```

### Interval type

::: code-group

```go [PostgreSQL]
// INTERVAL creates new interval expression from the list of quantity-unit pairs.
INTERVAL(1, YEAR, 10, MONTH)
INTERVAL(1, YEAR, 10, MONTH, 20, DAY, 3, HOUR)

// INTERVALd creates interval expression from time.Duration
INTERVALd(2*time.Hour + 3*time.Minute + 4*time.Second + 5*time.Microsecond)
```

```go [MySQL / MariaDB]
// INTERVAL creates new temporal interval.
// For MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR unit types
// the value parameter should be a number. For example: INTERVAL(1, DAY)
// For other unit types, value should be a string with the appropriate format.
// For example: INTERVAL("10:08:50", HOUR_SECOND)
INTERVAL(15, SECOND)
INTERVAL("25:15:08.000100", HOUR_MICROSECOND)

// INTERVALd creates temporal interval from duration
INTERVALd(3*time.Minute + 4*time.Second + 5*time.Microsecond)

// INTERVALe creates new Interval type from expression and unit type.
INTERVALe(Film.DurationInHours, HOUR)
```

:::

There is also:

```go
NULL
STAR // alias for *
```

## Column types

Every SQL builder table column belongs to one expression type. The column types are:

```go
ColumnBool
ColumnInteger
ColumnFloat
ColumnString
ColumnBlob / ColumnBytea
ColumnDate
ColumnTime
ColumnTimez
ColumnTimestamp
ColumnTimestampz
```

Columns and literals can form arbitrary expressions, but they have to follow valid SQL expression syntax.
Valid expressions:

```go
Bool(true).AND(Actor.IsActive).IS_FALSE()
(Film.Length.GT(Int(100)).AND(Film.Length.LT(Int(200)))).IS_TRUE()
```

Invalid expressions, which break `go build`:

```go
Bool(true).ADD(Int(11))        // can't add bool and integer
Int(11).LIKE(Float(22.2))      // integer expressions don't have a LIKE method
```

## Comparison operators

Jet supports the following comparison operators for all expression types:

| Method                 | Example                                          | Debug SQL                                                                          |
| ---------------------- | ------------------------------------------------ | ---------------------------------------------------------------------------------- |
| `EQ`                   | `Int(1).EQ(Film.Length)`                         | `1 = film.length`                                                                  |
| `NOT_EQ`               | `Int(1).NOT_EQ(Film.Length)`                     | `1 != film.length`                                                                 |
| `IS_DISTINCT_FROM`     | `Int(1).IS_DISTINCT_FROM(Film.Length)`           | PostgreSQL: `1 IS DISTINCT FROM film.length`, MySQL: `NOT(1 <=> film.length)`      |
| `IS_NOT_DISTINCT_FROM` | `Int(1).IS_NOT_DISTINCT_FROM(Film.Length)`       | PostgreSQL: `1 IS NOT DISTINCT FROM film.length`, MySQL: `1 <=> film.length`       |
| `LT`                   | `Int(1).LT(Film.Length)`                         | `1 < film.length`                                                                  |
| `LT_EQ`                | `Int(1).LT_EQ(Film.Length)`                      | `1 <= film.length`                                                                 |
| `GT`                   | `Int(1).GT(Film.Length)`                         | `1 > film.length`                                                                  |
| `GT_EQ`                | `Int(1).GT_EQ(Film.Length)`                      | `1 >= film.length`                                                                 |
| `BETWEEN`              | `Film.Length.BETWEEN(Int(100), Int(200))`        | `film.length BETWEEN 100 AND 200`                                                  |
| `NOT_BETWEEN`          | `Film.Length.NOT_BETWEEN(Int(50), Int(250))`     | `film.length NOT BETWEEN 50 AND 250`                                               |

_The left-hand side and right-hand side of an operator have to be of the same type._

## Arithmetic operators

The following arithmetic operators are supported for integer and float expressions. If the first argument is a
float expression, the second argument can be an integer or float expression. If the first argument is an
integer expression, the second argument can only be an integer expression.

| Method | Example                           | Debug SQL                 |
| ------ | --------------------------------- | ------------------------- |
| `ADD`  | `Int(1).ADD(Film.Length)`         | `1 + film.length`         |
| `SUB`  | `Float(1.11).SUB(Int(1))`         | `1.11 - 1`                |
| `MUL`  | `Int(1).MUL(Film.Length)`         | `1 * film.length`         |
| `DIV`  | `Float(1.11).DIV(Float(3.33))`    | `1.11 / 3.33`             |
| `MOD`  | `Int(10).MOD(Film.Length)`        | `10 % film.length`        |
| `POW`  | `Float(10.01).POW(Film.Length)`   | `POW(10.01, film.length)` |

## Bit operators

The following operators are only available on integer expressions:

| Method            | Example                                | Debug SQL                                                  |
| ----------------- | -------------------------------------- | ---------------------------------------------------------- |
| `BIT_AND`         | `Int(11).BIT_AND(Film.Length)`         | `11 & film.length`                                         |
| `BIT_OR`          | `Int(11).BIT_OR(Film.Length)`          | `11 \| film.length`                                        |
| `BIT_XOR`         | `Int(11).BIT_XOR(Film.Length)`         | PostgreSQL: `11 # film.length`, MySQL: `11 ^ film.length`  |
| `BIT_NOT`         | `BIT_NOT(Film.Length)`                 | `~ film.length`                                            |
| `BIT_SHIFT_LEFT`  | `Int(11).BIT_SHIFT_LEFT(Film.Length)`  | `11 << film.length`                                        |
| `BIT_SHIFT_RIGHT` | `Int(11).BIT_SHIFT_RIGHT(Film.Length)` | `11 >> film.length`                                        |

## Logical operators

The following operators are only available on boolean expressions:

| Method           | Example                                        | Debug SQL                             |
| ---------------- | ---------------------------------------------- | ------------------------------------- |
| `IS_TRUE`        | `Staff.Active.IS_TRUE()`                       | `staff.active IS TRUE`                |
| `IS_NOT_TRUE`    | `(Staff.Active.AND(Bool(true))).IS_NOT_TRUE()` | `(staff.active AND true) IS NOT TRUE` |
| `IS_FALSE`       | `Bool(false).IS_FALSE()`                       | `false IS FALSE`                      |
| `IS_NOT_FALSE`   | `Bool(true).IS_NOT_FALSE()`                    | `true IS NOT FALSE`                   |
| `IS_UNKNOWN`     | `Staff.Active.IS_UNKNOWN()`                    | `staff.active IS UNKNOWN`             |
| `IS_NOT_UNKNOWN` | `Staff.Active.IS_NOT_UNKNOWN()`                | `staff.active IS NOT UNKNOWN`         |
| `AND`            | `Staff.Active.AND(Account.Active)`             | `staff.active AND account.active`     |
| `OR`             | `Staff.Active.OR(Account.Active)`              | `staff.active OR account.active`      |

There are also global functions `AND` and `OR`, which allow
[better indentation](https://github.com/go-jet/jet/blob/c29f0afd2bbc8364d6f1d46be24d1e4decb39616/tests/postgres/chinook_db_test.go#L51)
of complex conditions both in Go code and in the generated SQL.

## String operators

The following operators are only available on string expressions:

| Method            | Example                                      | Debug SQL                                                                     |
| ----------------- | -------------------------------------------- | ----------------------------------------------------------------------------- |
| `CONCAT`          | `Film.Name.CONCAT(Film.Description)`         | `film.name \|\| film.description`                                             |
| `LIKE`            | `Film.Name.LIKE(String("%Wind%"))`           | `film.name LIKE '%Wind%'`                                                     |
| `NOT_LIKE`        | `Film.Name.NOT_LIKE(String("%Wind%"))`       | `film.name NOT LIKE '%Wind%'`                                                 |
| `REGEXP_LIKE`     | `Film.Name.REGEXP_LIKE(String("^Wind"))`     | PostgreSQL: `film.name ~* '^Wind'`, MySQL: `film.name REGEXP '^Wind'`         |
| `NOT_REGEXP_LIKE` | `Film.Name.NOT_REGEXP_LIKE(String("^Wind"))` | PostgreSQL: `film.name !~* '^Wind'`, MySQL: `film.name NOT REGEXP '^Wind'`    |

## Row (tuple) expressions

The `ROW` constructor builds a row value (tuple) from the expressions provided as parameters:

```go
ROW(Actor.ActorID, Actor.FirstName, Int(0)) // ROW(actor.actor_id, actor.first_name, 0)
```

Row expressions are commonly used for tuple comparison:

```go
SELECT(Actor.AllColumns).
FROM(Actor).
WHERE(
    ROW(Actor.ActorID, Actor.FirstName).IN(
        ROW(Int(1), String("Joe")),
        ROW(Int(2), String("Nick")),
    ),
)
```

## SQL cast operators

Cast operators allow expressions to be cast to another database type. The SQL builder expression type changes
accordingly.

| Method                      | Example                                       | PostgreSQL generated SQL                        |
| --------------------------- | --------------------------------------------- | ----------------------------------------------- |
| `CAST(exp).AS_BOOL()`       | `CAST(Film.Description).AS_BOOL()`            | `film.description::boolean`                     |
| `CAST(exp).AS_SMALLINT()`   | `CAST(Film.Description).AS_SMALLINT()`        | `film.description::smallint`                    |
| `CAST(exp).AS_INTEGER()`    | `CAST(Film.Description).AS_INTEGER()`         | `film.description::integer`                     |
| `CAST(exp).AS_BIGINT()`     | `CAST(Film.Description).AS_BIGINT()`          | `film.description::bigint`                      |
| `CAST(exp).AS_NUMERIC()`    | `CAST(Film.Description).AS_NUMERIC(10, 6)`    | `film.description::numeric(10,6)`               |
| `CAST(exp).AS_REAL()`       | `CAST(Film.Description).AS_REAL()`            | `film.description::real`                        |
| `CAST(exp).AS_DOUBLE()`     | `CAST(Film.Description).AS_DOUBLE()`          | `film.description::double precision`            |
| `CAST(exp).AS_TEXT()`       | `CAST(Film.Description).AS_TEXT()`            | `film.description::text`                        |
| `CAST(exp).AS_DATE()`       | `CAST(Film.Description).AS_DATE()`            | `film.description::date`                        |
| `CAST(exp).AS_TIME()`       | `CAST(Film.Description).AS_TIME()`            | `film.description::time without time zone`      |
| `CAST(exp).AS_TIMEZ()`      | `CAST(Film.Description).AS_TIMEZ()`           | `film.description::time with time zone`         |
| `CAST(exp).AS_TIMESTAMP()`  | `CAST(Film.Description).AS_TIMESTAMP()`       | `film.description::timestamp without time zone` |
| `CAST(exp).AS_TIMESTAMPZ()` | `CAST(Film.Description).AS_TIMESTAMPZ()`      | `film.description::timestamp with time zone`    |

MySQL and MariaDB generate SQL in the form `CAST(exp AS integer)`.

## SQL builder cast wrappers

For some expressions the SQL builder cannot deduce the expression type directly. A scalar sub-query, for
instance:

```go
Float(11.1).LT(
    SELECT(MAX(Film.RentalRate)).
    FROM(Film),
)
```

This does not compile: the sub-query evaluates to a single float value, but it is not a float expression. To
fix it, the sub-query can be cast to a float type, or wrapped as a float expression:

```go
Float(11.1).LT(FloatExp(
    SELECT(MAX(Film.RentalRate)).
    FROM(Film),
))
```

There are wrappers for all supported types:

```go
BoolExp(exp)
IntExp(exp)
FloatExp(exp)
StringExp(exp)
BlobExp(exp) / ByteaExp(exp)
DateExp(exp)
TimeExp(exp)
TimezExp(exp)
TimestampExp(exp)
TimestampzExp(exp)
```

::: warning
Cast wrappers do NOT inject a cast operator into the generated SQL. They only change the Go type.
:::

## Raw expression

Raw expressions can be used for any unsupported function, operator or expression:

```go
Raw("current_database()")
Raw("(#duration + film.duration) / $arg", RawArgs{"#duration": 11, "$arg": 200})
```

`RawArgs` contains named arguments for placeholders in the raw query. The naming convention is free-form, the
names just have to match the placeholders in the raw query exactly. It is recommended NOT to use `$1, $2, ...`
for PostgreSQL queries.

A `Raw` expression can be cast or wrapped to the desired expression type, or one of the typed helpers can be
used: `RawInt`, `RawFloat`, `RawString`, `RawBool`, etc.

See also [custom functions and operators](../faq#how-to-use-custom-or-currently-unsupported-functions-and-operators)
in the FAQ.
