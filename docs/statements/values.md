# VALUES

`VALUES` creates an inline temporary table from a list of rows. It can be used as a sub-query in a `FROM`
clause, or as the body of a [CTE](./with).

## PostgreSQL and CockroachDB

### As a sub-query

```go
paymentID := IntegerColumn("payment_ID")
increase := FloatColumn("increase")

values := VALUES(
	WRAP(Int32(20564), Double(1.21)),
	WRAP(Int32(20567), Double(1.02)),
	WRAP(Int32(20570), Double(1.34)),
	WRAP(Int32(20573), Double(1.72)),
).AS("values", paymentID, increase)

stmt := SELECT(Payment.AllColumns).
	FROM(Payment.INNER_JOIN(values, paymentID.EQ(Payment.PaymentID))).
	WHERE(increase.GT(Double(1.03)))
```

### As a CTE

```go
paymentID := IntegerColumn("payment_ID")
increase := FloatColumn("increase")
paymentsToUpdate := CTE("values_cte", paymentID, increase)

stmt := WITH(
	paymentsToUpdate.AS(
		VALUES(
			WRAP(Int32(20564), Double(1.21)),
			WRAP(Int32(20567), Double(1.02)),
			WRAP(Int32(20570), Double(1.34)),
			WRAP(Int32(20573), Double(1.72)),
		),
	),
)(
	Payment.UPDATE().
	SET(
		Payment.Amount.SET(Payment.Amount.MUL(increase)),
	).
	FROM(paymentsToUpdate).
	WHERE(Payment.PaymentID.EQ(paymentID)).
	RETURNING(Payment.AllColumns),
)
```

::: warning
The `ROW` constructor creates a row expression, but it is not a substitute for `WRAP` in a PostgreSQL `VALUES`
context. Also, `Int` and `Float` cannot be used for `VALUES` elements, because they do not add an explicit type
cast. Use specific types such as `Int32`, `Int64`, `Real`, `Double`, etc.
:::

## MySQL

### As a sub-query

```go
paymentID := IntegerColumn("payment_id")
increase := FloatColumn("increase")

values := VALUES(
	ROW(Int32(204), Float(1.21)),
	ROW(Int32(207), Float(1.02)),
	ROW(Int32(200), Float(1.34)),
	ROW(Int32(203), Float(1.72)),
).AS("values", paymentID, increase)

stmt := SELECT(Payment.AllColumns).
	FROM(Payment.INNER_JOIN(values, paymentID.EQ(Payment.PaymentID))).
	WHERE(increase.GT(Float(1.03)))
```

### As a CTE

```go
paymentID := IntegerColumn("payment_id")
increase := FloatColumn("increase")
paymentsToUpdate := CTE("values_cte", paymentID, increase)

stmt := WITH(
	paymentsToUpdate.AS(
		VALUES(
			ROW(Int32(204), Float(1.21)),
			ROW(Int32(207), Float(1.02)),
			ROW(Int32(200), Float(1.34)),
			ROW(Int32(203), Float(1.72)),
		),
	),
)(
	Payment.INNER_JOIN(paymentsToUpdate, paymentID.EQ(Payment.PaymentID)).
		UPDATE().
		SET(
			Payment.Amount.SET(Payment.Amount.MUL(increase)),
		).WHERE(Bool(true)),
)
```

## SQLite

### As a sub-query

```go
values := VALUES(
	ROW(Int32(204), Float(1.21)),
	ROW(Int32(207), Float(1.02)),
	ROW(Int32(200), Float(1.34)),
	ROW(Int32(203), Float(1.72)),
).AS("values")

paymentID := IntegerColumn("column1")
increase := FloatColumn("column2")

stmt := SELECT(Payment.AllColumns).
	FROM(Payment.INNER_JOIN(values, paymentID.EQ(Payment.PaymentID))).
	WHERE(increase.GT(Float(1.03)))
```

::: info
SQLite does not support column aliasing when `VALUES` is used as a sub-query. Reference the columns by their
default names `column1`, `column2`, ..., `columnN`, as in the example above. When column aliasing is required,
use the CTE approach.
:::

### As a CTE

```go
paymentID := IntegerColumn("payment_ID")
increase := FloatColumn("increase")
paymentsToUpdate := CTE("values_cte", paymentID, increase)

stmt := WITH(
	paymentsToUpdate.AS(
		VALUES(
			ROW(Int32(204), Float(1.21)),
			ROW(Int32(207), Float(1.02)),
			ROW(Int32(200), Float(1.34)),
			ROW(Int32(203), Float(1.72)),
		),
	),
)(
	Payment.UPDATE().
		SET(
			Payment.Amount.SET(Payment.Amount.MUL(increase)),
		).
		FROM(paymentsToUpdate).
		WHERE(Payment.PaymentID.EQ(paymentID)).
		RETURNING(Payment.AllColumns),
)
```

## MariaDB

In MariaDB, `VALUES` statements do not work correctly when all elements are placeholders. A workaround with
`RawStatement` is required:

```go
paymentID := IntegerColumn("payment_id")
increase := FloatColumn("increase")
paymentsToUpdate := CTE("values_cte", paymentID, increase)

stmt := WITH(
	paymentsToUpdate.AS(
		RawStatement(`
			 VALUES (204, 1.21),
				(207, 1.02),
				(200, 1.34),
				(203, 1.72)
		`),
	),
)(
	SELECT(
		Payment.AllColumns,
		paymentsToUpdate.AllColumns(),
	).FROM(
		Payment.INNER_JOIN(paymentsToUpdate, paymentID.EQ(Payment.PaymentID)),
	).WHERE(
		increase.GT(Float(1.03)),
	).ORDER_BY(
		increase,
	),
)
```
