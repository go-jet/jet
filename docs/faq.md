# FAQ

## How to execute a jet statement in a SQL transaction?

```go
tx, err := db.Begin()
...
stmt := SELECT(...)

var dest Dest
err = stmt.QueryContext(ctx, tx, &dest) // or stmt.ExecContext(ctx, tx)
...
tx.Commit()
```

More about statement execution in [Statements](./statements/#executing-statements).

## How to construct a dynamic projection list?

```go
var request struct {
	ColumnsToSelect []string
	ShowFullName    bool
}
// ...

var projectionList ProjectionList

for _, columnName := range request.ColumnsToSelect {
	switch columnName {
	case Customer.CustomerID.Name():
		projectionList = append(projectionList, Customer.CustomerID)
	case Customer.Email.Name():
		projectionList = append(projectionList, Customer.Email)
	case Customer.CreateDate.Name():
		projectionList = append(projectionList, Customer.CreateDate)
	}
}

if request.ShowFullName {
	projectionList = append(projectionList, Customer.FirstName.CONCAT(Customer.LastName))
}

stmt := SELECT(projectionList).
	FROM(Customer).
	LIMIT(3)
```

## How to construct a dynamic condition?

```go
var request struct {
	CustomerID *int64
	Email      *string
	Active     *bool
}

// ....

condition := Bool(true)

if request.CustomerID != nil {
	condition = condition.AND(Customer.CustomerID.EQ(Int(*request.CustomerID)))
}
if request.Email != nil {
	condition = condition.AND(Customer.Email.EQ(String(*request.Email)))
}
if request.Active != nil {
	condition = condition.AND(Customer.Activebool.EQ(Bool(*request.Active)))
}

stmt := SELECT(Customer.AllColumns).
	FROM(Customer).
	WHERE(condition)
```

## How to use jet in a multi-tenant environment?

The default schema targeted by a generated table can be changed with `FromSchema`:

```go
multiTenant1 :=
	SELECT(Artist.AllColumns).
	FROM(Artist).                        // default schema/database "chinook"
	ORDER_BY(Artist.ArtistId).
	LIMIT(10)

Artist2 := Artist.FromSchema("chinook2") // the same generated type used for a different schema/database

multiTenant2 :=
	SELECT(Artist2.AllColumns).
	FROM(Artist2).
	ORDER_BY(Artist2.ArtistId).
	LIMIT(10)
```

Alternatively, the default schema of every table and view can be changed globally with `UseSchema`:

```go
table.UseSchema("chinook2")
view.UseSchema("chinook2")
```

## How to change a model field type?

### Generator customization

By default the generator represents exact decimal types (`DECIMAL` and `NUMERIC`) as `float64` fields, which can
lose precision during query result mapping.

To fix this, create a new type to store decimal values and instruct the
[generator to use it instead of `float64`](./guide/generator#generator-customization).

**The new type has to implement the `sql.Scanner` and `driver.Valuer` interfaces.**

```go
type MoneyType int64 // or some other representation

func (m *MoneyType) Scan(value interface{}) error { // value is string
	... // add implementation
}

func (m MoneyType) Value() (driver.Value, error) {
	... // add implementation
}
```

### Wrap generated types

Similar behavior can be achieved without customizing the generator. Assuming table `my_table` has a `money`
column of type `NUMERIC`, wrap the generated `MyTable` type:

```go
type MyTable struct {
    model.MyTable   // MyTable.Money contains the float64 value
    Money MoneyType // shadows model.MyTable.Money with the exact decimal value
}
```

The new `MyTable` type can be used anywhere `model.MyTable` is used: as a `QueryContext` destination or as a
model for `INSERT` and `UPDATE` statements.

Existing third-party decimal libraries work too:

```go
import "github.com/shopspring/decimal"

type MyTable struct {
    model.MyTable
    Money decimal.Decimal
}
```

## How to use custom (or currently unsupported) functions and operators?

Most database functions and operators are supported. For the missing ones, Jet exposes enough of its internals
to add them yourself.

### Scalar functions

Scalar functions return a single value of a specific data type (`INTEGER`, `VARCHAR`, `DATE`, ...). Suppose
the database contains:

```sql
CREATE FUNCTION get_film_count(len_from int, len_to int) RETURNS int
```

Define a utility function:

```go
func GET_FILM_COUNT(lenFrom, lenTo IntegerExpression) IntegerExpression { // or (lenFrom, lenTo int) if a column
	return IntExp(Func("dvds.get_film_count", lenFrom, lenTo))            // or expression parameter is never needed
}
```

And call it directly from a query:

```go
stmt := SELECT(
	GET_FILM_COUNT(Int(100), Int(120)).AS("film_count"),
)
```

### Set-returning functions

Set-returning functions return a set of rows. Suppose the database contains:

```sql
CREATE FUNCTION dvds.film_in_stock(p_film_id integer, p_store_id integer, OUT p_film_count integer) RETURNS SETOF integer
```

To use it as a table source in a `FROM` clause, wrap a raw statement in a CTE:

```go
inventoryID := IntegerColumn("inventoryID")
filmsInStock := CTE("film_in_stock", inventoryID)

stmt := WITH(
	filmsInStock.AS(
		RawStatement("SELECT * FROM dvds.film_in_stock(#filmID, #storeID)",
			RawArgs{
				"#filmID":  1,
				"#storeID": 2,
			}),
	),
)(
	SELECT(
		Inventory.AllColumns,
	).FROM(Inventory.
		INNER_JOIN(filmsInStock, Inventory.InventoryID.EQ(inventoryID)),
	),
)
```

### Operators

Operators can be defined as utility functions with `CustomExpression` and `Token`. For example, a
case-insensitive `ILIKE`:

```go
func ILIKE(lhs, rhs StringExpression) BoolExpression {
	return BoolExp(CustomExpression(lhs, Token("ILIKE"), rhs))
}
```

Usage:

```go
stmt := SELECT(
	ILIKE(String("FOOFoo"), String("foo%")).AS("foo_like"),
)
```

### Raw expression fallback

If none of the above fits, the same effect can be achieved with a [Raw](./guide/expressions#raw-expression)
expression:

```go
stmt2 := SELECT(
	Raw("dvds.get_film_count(#1, #2)", RawArgs{"#1": 100, "#2": 120}).AS("film_count"),
)
```

### Raw statement fallback

At the cost of all Jet benefits, the entire statement can be written as a
[RawStatement](./statements/#raw-statements):

```go
stmt3 := RawStatement(`
	SELECT dvds.get_film_count(#1, #2) AS "film_count";`, RawArgs{"#1": 100, "#2": 120},
)
```

## How to use IN / NOT_IN with a dynamic list of values?

```go
import (
	. "myapp/table"
	. "github.com/go-jet/jet/v2/sqlite"
)

func demo() {
	var userIDs = []int64{1, 2, 3} // dynamic list, could be user provided
	var sqlIDs []Expression        // !!! must be []Expression !!!

	for _, userID := range userIDs {
		sqlIDs = append(sqlIDs, Int(userID))
	}

	SELECT(
		Users.AllColumns,
	).FROM(
		Users,
	).WHERE(
		Users.UserID.IN(sqlIDs...),
	)

	...
}
```

For tuple comparison use `ROW`:

```go
ROW(Users.UserID, Users.Name).IN(
    ROW(Int(1), String("John")),
    ROW(Int(2), String("Mike")),
)
```

`sqlIDs` must be of type `[]Expression` or compilation fails. _This is a known limitation that will be fixed in
v3, where the right-hand argument of `IN` / `NOT_IN` will match the type of the left-hand argument._

## Scan stopped working after naming a destination type

Developers usually start with a query like this:

```go
stmt := SELECT(
	Payment.PaymentID,                    // no need to alias, "payment.payment_id" alias is added automatically
	MAX(Payment.Amount).AS("max_amount"), // alias equivalent to ".max_amount"
).FROM(
	Payment,
).WHERE(
	Payment.CustomerID.EQ(Int(101)),
).GROUP_BY(
	Payment.PaymentID,
)

var dest struct {
	model.Payment
	MaxAmount int
}

err := stmt.QueryContext(ctx, db, &dest)
```

This scan works for `MaxAmount`, because there is a valid mapping between the alias and the destination field:
alias `.max_amount` maps to field `.MaxAmount`. Note that the destination is an anonymous type.

After naming the destination type, the mapping breaks:

```go
type MyStruct struct {
	model.Payment
	MaxAmount int
}

var dest MyStruct
```

Alias `.max_amount` no longer maps to `MyStruct.MaxAmount`. To fix it, update the alias in the query:

```go
SELECT(
	Payment.PaymentID,                              // still no need to alias
	MAX(Payment.Amount).AS("my_struct.max_amount"), // ".max_amount" -> "my_struct.max_amount"
).FROM(
	Payment,
)...
```

_The same applies if `dest` is a slice of a named type._
