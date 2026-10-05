# WITH

`WITH` attaches auxiliary statements, called Common Table Expressions (CTEs), to a larger query. A CTE can be
thought of as a temporary table that exists for the duration of one query.
Reference documentation:
[PostgreSQL](https://www.postgresql.org/docs/current/queries-with.html),
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/with.html),
[MariaDB](https://mariadb.com/kb/en/with),
[SQLite](https://www.sqlite.org/lang_with.html).

## How to write WITH statements

- A CTE has to be declared with `CTE(name)` before the `WITH` statement.
- A CTE can be:
  - PostgreSQL: a `SELECT`, `INSERT`, `UPDATE` or `DELETE` statement.
  - MySQL, SQLite: only a `SELECT` statement, but the main statement can be `SELECT`, `UPDATE` or `DELETE`.
  - MariaDB: CTEs and the main statement can only be `SELECT` statements.
- CTE exported columns can be defined before the `WITH` statement, for readability.

### PostgreSQL example

```go
// CTE declarations
removeDiscontinuedOrders := CTE("remove_discontinued_orders")
updateDiscontinuedPrice := CTE("update_discontinued_price")
logDiscontinuedProducts := CTE("log_discontinued")

// CTE exported column. Can be used in other CTEs (updateDiscontinuedPrice) or in the main statement.
discontinuedProductID := OrderDetails.ProductID.From(removeDiscontinuedOrders)

stmt := WITH(
    removeDiscontinuedOrders.AS(
        OrderDetails.DELETE().
        WHERE(OrderDetails.ProductID.IN(
                SELECT(Products.ProductID).
                FROM(Products).
                WHERE(Products.Discontinued.EQ(Int(1))),
            ),
        ).RETURNING(OrderDetails.ProductID),
    ),
    updateDiscontinuedPrice.AS(
        Products.UPDATE().
        SET(
            Products.UnitPrice.SET(Float(0.0)),
        ).
        WHERE(Products.ProductID.IN(removeDiscontinuedOrders.SELECT(discontinuedProductID))).
        RETURNING(Products.AllColumns),
    ),
    logDiscontinuedProducts.AS(
        ProductLogs.INSERT(ProductLogs.AllColumns).
        QUERY(
               SELECT(updateDiscontinuedPrice.AllColumns()).
               FROM(updateDiscontinuedPrice),
        ).
        RETURNING(ProductLogs.AllColumns),
    ),
)(
    SELECT(logDiscontinuedProducts.AllColumns()).
    FROM(logDiscontinuedProducts),
)

var resp []model.ProductLogs
err = stmt.Query(tx, &resp)
```

::: info
CTE projection aliasing follows the same rules as [sub-queries](./subquery).
:::

### MySQL and MariaDB example

```go
salesRep := CTE("sales_rep")
customerSalesRep := CTE("customer_sales_rep")

salesRepStaffID := Staff.StaffID.From(salesRep)
salesRepFullName := StringColumn("sales_rep_full_name").From(salesRep)

stmt := WITH(
    salesRep.AS(
        SELECT(
            Staff.StaffID,
            Staff.FirstName.CONCAT(Staff.LastName).AS(salesRepFullName.Name()),
        ).FROM(Staff),
    ),
    customerSalesRep.AS(
        SELECT(
            Customer.FirstName.CONCAT(Customer.LastName).AS("customer_name"),
            salesRepFullName,
        ).FROM(
            salesRep.
                INNER_JOIN(Store, Store.ManagerStaffID.EQ(salesRepStaffID)).
                INNER_JOIN(Customer, Customer.StoreID.EQ(Store.StoreID)),
        ),
    ),
)(
    SELECT(customerSalesRep.AllColumns()).
    FROM(customerSalesRep),
)

var dest []struct {
    CustomerName     string
    SalesRepFullName string
}
err := stmt.Query(db, &dest)
```

## WITH RECURSIVE

With `RECURSIVE`, a `WITH` query can refer to its own output. The general form is a non-recursive term, then
`UNION` (or `UNION ALL`), then a recursive term, where only the recursive term can reference the query's own
output.

```go
subordinates := CTE("subordinates")

stmt := WITH_RECURSIVE(
	subordinates.AS(
		SELECT(
			Employees.AllColumns,
		).FROM(
			Employees,
		).WHERE(
			Employees.EmployeeID.EQ(Int(2)),
		).UNION(
			SELECT(
				Employees.AllColumns,
			).FROM(
				Employees.
					INNER_JOIN(subordinates, Employees.EmployeeID.From(subordinates).EQ(Employees.ReportsTo)),
			),
		),
	),
)(
	SELECT(
		subordinates.AllColumns(),
	).FROM(
		subordinates,
	),
)
```
