# SELECT_JSON

`SELECT` statements over many joined tables return a de-normalized result set: the row count grows rapidly and
most of the data is duplicated. Below about 1,000 rows this is not a concern, but past roughly 10,000 rows the
performance impact becomes noticeable.

One way to avoid transferring duplicate data is to encode the result as JSON on the SQL server. Instead of a
large set of rows, the query returns a single row with a single column containing the entire result.

Depending on the amount of duplication and the distance between the SQL server and the application, this can
yield several-fold performance improvements over standard queries.

Jet provides built-in support for JSON results via two statement types: `SELECT_JSON_OBJ` and
`SELECT_JSON_ARR`.

## SELECT_JSON_OBJ

`SELECT_JSON_OBJ` constructs a single JSON object from the selected columns.

```go
stmt := SELECT_JSON_OBJ(Actor.AllColumns).
	FROM(Actor).
	WHERE(Actor.ActorID.EQ(Int32(2)))
```

The destination must be a pointer to a struct or a `map[string]any`:

```go
var dest model.Actor

err := stmt.QueryContext(ctx, db, &dest)
```

::: details Generated SQL

PostgreSQL / CockroachDB:

```sql
SELECT row_to_json(records) AS "json"
FROM (
          SELECT actor.actor_id AS "actorID",
               actor.first_name AS "firstName",
               actor.last_name AS "lastName",
               to_char(actor.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
          FROM dvds.actor
          WHERE actor.actor_id = $1::integer
     ) AS records;
```

MySQL / MariaDB:

```sql
SELECT JSON_OBJECT(
          'actorID', actor.actor_id,
          'firstName', actor.first_name,
          'lastName', actor.last_name,
          'lastUpdate', DATE_FORMAT(actor.last_update,'%Y-%m-%dT%H:%i:%s.%fZ')
     ) AS "json"
FROM dvds.actor
WHERE actor.actor_id = ?;
```

:::

## SELECT_JSON_ARR

`SELECT_JSON_ARR` constructs a JSON array of objects from the selected columns.

```go
stmt := SELECT_JSON_ARR(Rental.AllColumns).
	FROM(Rental).
	WHERE(Rental.CustomerID.LT(Int(2))).
	ORDER_BY(Rental.StaffID.ASC())
```

The destination must be a pointer to a slice of structs (for example `[]model.Rental`) or a slice of maps
(`[]map[string]any`):

```go
var dest []model.Rental

err := stmt.QueryContext(ctx, db, &dest)
```

::: details Generated SQL

PostgreSQL / CockroachDB:

```sql
SELECT json_agg(row_to_json(records)) AS "json"
FROM (
          SELECT rental.rental_id AS "rentalID",
               to_char(rental.rental_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "rentalDate",
               rental.inventory_id AS "inventoryID",
               rental.customer_id AS "customerID",
               to_char(rental.return_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "returnDate",
               rental.staff_id AS "staffID",
               to_char(rental.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
          FROM dvds.rental
          WHERE rental.customer_id < $1
          ORDER BY rental.staff_id ASC
     ) AS records;
```

MySQL / MariaDB:

```sql
SELECT JSON_ARRAYAGG(JSON_OBJECT(
          'rentalID', rental.rental_id,
          'rentalDate', DATE_FORMAT(rental.rental_date,'%Y-%m-%dT%H:%i:%s.%fZ'),
          'inventoryID', rental.inventory_id,
          'customerID', rental.customer_id,
          'returnDate', DATE_FORMAT(rental.return_date,'%Y-%m-%dT%H:%i:%s.%fZ'),
          'staffID', rental.staff_id,
          'lastUpdate', DATE_FORMAT(rental.last_update,'%Y-%m-%dT%H:%i:%s.%fZ')
     )) AS "json"
FROM dvds.rental
WHERE rental.customer_id < ?
ORDER BY rental.staff_id ASC;
```

:::

::: info
`SELECT_JSON` statements support the same clauses as regular `SELECT` statements, making them easily
interchangeable.
:::

## Combining SELECT_JSON statements

### Nested SELECT_JSON statements

`SELECT_JSON` statements can be nested to create complex JSON objects:

```go
stmt := SELECT_JSON_OBJ(
	Customer.AllColumns,

	SELECT_JSON_ARR(Rental.AllColumns).
		FROM(Rental).
		WHERE(Rental.CustomerID.EQ(Customer.CustomerID)).
		ORDER_BY(Rental.RentalID).
		OFFSET(1).AS("Rentals"),
).FROM(
	Customer,
).WHERE(
	Customer.CustomerID.EQ(Int32(11)),
)

var dest struct {
	model.Customer

	Rentals []model.Rental
}

err := stmt.QueryContext(ctx, db, &dest)
```

::: details Generated SQL

PostgreSQL / CockroachDB:

```sql
SELECT row_to_json(records) AS "json"
FROM (
          SELECT customer.customer_id AS "customerID",
               customer.store_id AS "storeID",
               customer.first_name AS "firstName",
               customer.last_name AS "lastName",
               customer.email AS "email",
               customer.address_id AS "addressID",
               customer.activebool AS "activebool",
               to_char(customer.create_date::timestamp, 'YYYY-MM-DD') || 'T00:00:00Z' AS "createDate",
               to_char(customer.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate",
               customer.active AS "active",
               (
                    SELECT json_agg(row_to_json(rentals_records)) AS "rentals_json"
                    FROM (
                              SELECT rental.rental_id AS "rentalID",
                                   to_char(rental.rental_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "rentalDate",
                                   rental.inventory_id AS "inventoryID",
                                   rental.customer_id AS "customerID",
                                   to_char(rental.return_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "returnDate",
                                   rental.staff_id AS "staffID",
                                   to_char(rental.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
                              FROM dvds.rental
                              WHERE rental.customer_id = customer.customer_id
                              ORDER BY rental.rental_id
                              OFFSET $1
                         ) AS rentals_records
               ) AS "Rentals"
          FROM dvds.customer
          WHERE customer.customer_id = $2::integer
     ) AS records;
```

MySQL / MariaDB:

```sql
SELECT JSON_OBJECT(
          'customerID', customer.customer_id,
          'storeID', customer.store_id,
          'firstName', customer.first_name,
          'lastName', customer.last_name,
          'email', customer.email,
          'addressID', customer.address_id,
          'active', customer.active = 1,
          'createDate', DATE_FORMAT(customer.create_date,'%Y-%m-%dT%H:%i:%s.%fZ'),
          'lastUpdate', DATE_FORMAT(customer.last_update,'%Y-%m-%dT%H:%i:%s.%fZ'),
          'Rentals', (
               SELECT JSON_ARRAYAGG(JSON_OBJECT(
                         'rentalID', rental.rental_id,
                         'rentalDate', DATE_FORMAT(rental.rental_date,'%Y-%m-%dT%H:%i:%s.%fZ'),
                         'inventoryID', rental.inventory_id,
                         'customerID', rental.customer_id,
                         'returnDate', DATE_FORMAT(rental.return_date,'%Y-%m-%dT%H:%i:%s.%fZ'),
                         'staffID', rental.staff_id,
                         'lastUpdate', DATE_FORMAT(rental.last_update,'%Y-%m-%dT%H:%i:%s.%fZ')
                    )) AS "json"
               FROM dvds.rental
               WHERE rental.customer_id = customer.customer_id
               ORDER BY rental.rental_id
               OFFSET ?
          )
     ) AS "json"
FROM dvds.customer
WHERE customer.customer_id = ?;
```

Args: `[1 11]`

:::

A nested `SELECT_JSON` statement behaves like a `LEFT JOIN`: here `Customer` is joined with `Rental` even though
no explicit `LEFT_JOIN` clause is used.

Each nested `SELECT_JSON` used as a projection must have an alias (otherwise the query panics). The alias
becomes the key in the resulting JSON object and must match the corresponding struct field name.

::: info
There is no limit on how deeply `SELECT_JSON` statements can be nested.
:::

### Combining SELECT_JSON with other statements

`SELECT_JSON` statements can also be combined with `SELECT`, `INSERT`, `UPDATE` and `DELETE` to return hybrid
results. The following query returns `Rentals` as a JSON array while `Customer` is returned as a regular
result set:

```go
stmt := SELECT(
	Customer.AllColumns,

	SELECT_JSON_ARR(Rental.AllColumns).
		FROM(Rental).
		WHERE(Rental.CustomerID.EQ(Customer.CustomerID)).
		ORDER_BY(Rental.RentalID).
		OFFSET(1).AS("Rentals"),
).FROM(
	Customer,
).WHERE(
	Customer.CustomerID.EQ(Int32(11)),
)

var dest struct {
	model.Customer

	Rentals []model.Rental `json_column:"Rentals"` // !! json_column tag
}

err := stmt.QueryContext(ctx, db, &dest)
```

The query may return multiple rows and multiple columns, one of which is a JSON column named `Rentals`. For the
JSON column to be scanned correctly, the destination field must be tagged with `json_column`.

::: details Generated SQL

PostgreSQL / CockroachDB:

```sql
SELECT customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.activebool AS "customer.activebool",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     customer.active AS "customer.active",
     (
          SELECT json_agg(row_to_json(rentals_records)) AS "rentals_json"
          FROM (
                    SELECT rental.rental_id AS "rentalID",
                         to_char(rental.rental_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "rentalDate",
                         rental.inventory_id AS "inventoryID",
                         rental.customer_id AS "customerID",
                         to_char(rental.return_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "returnDate",
                         rental.staff_id AS "staffID",
                         to_char(rental.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
                    FROM dvds.rental
                    WHERE rental.customer_id = customer.customer_id
                    ORDER BY rental.rental_id
                    OFFSET $1
               ) AS rentals_records
     ) AS "Rentals"
FROM dvds.customer
WHERE customer.customer_id = $2::integer;
```

MySQL / MariaDB:

```sql
SELECT customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.active AS "customer.active",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     (
          SELECT JSON_ARRAYAGG(JSON_OBJECT(
                    'rentalID', rental.rental_id,
                    'rentalDate', DATE_FORMAT(rental.rental_date,'%Y-%m-%dT%H:%i:%s.%fZ'),
                    'inventoryID', rental.inventory_id,
                    'customerID', rental.customer_id,
                    'returnDate', DATE_FORMAT(rental.return_date,'%Y-%m-%dT%H:%i:%s.%fZ'),
                    'staffID', rental.staff_id,
                    'lastUpdate', DATE_FORMAT(rental.last_update,'%Y-%m-%dT%H:%i:%s.%fZ')
               )) AS "json"
          FROM dvds.rental
          WHERE rental.customer_id = customer.customer_id
          ORDER BY rental.rental_id
          OFFSET ?
     ) AS "Rentals"
FROM dvds.customer
WHERE customer.customer_id = ?;
```

Args: `[1 11]`

:::

## Raw JSON

When no further processing of the JSON is required, unmarshaling can be skipped entirely and the JSON forwarded
directly to the client, for a small additional performance gain.

Wrap the `SELECT_JSON` query in a regular `SELECT` so that QRM scans it as a plain column:

```go
stmt := SELECT( // !! SELECT statement
	SELECT_JSON_OBJ(
		Customer.AllColumns,

		SELECT_JSON_ARR(Rental.AllColumns).
			FROM(Rental).
			WHERE(Rental.CustomerID.EQ(Customer.CustomerID)).
			ORDER_BY(Rental.RentalID).
			OFFSET(1).AS("Rentals"),
	).FROM(
		Customer,
	).WHERE(
		Customer.CustomerID.EQ(Int32(11)),
	).AS("raw_json"), // !! json column alias
)

var dest struct {
	RawJson []byte
}

err := stmt.Query(db, &dest)

// .........................

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
w.Write(dest.RawJson)
```

The query returns the raw JSON as `[]byte`, which is written directly to the HTTP response.

::: info
The generated JSON follows RFC 7159, the same format used by the standard library `encoding/json` package.
:::

## Custom JSON unmarshaler

By default `SELECT_JSON` results are unmarshaled with `json.Unmarshal` from `encoding/json`. A different
library can be configured during application initialization. For example, with
[`github.com/bytedance/sonic`](https://github.com/bytedance/sonic):

```go
import (
    "github.com/go-jet/jet/v2/qrm"
    "github.com/bytedance/sonic"
)

func ServerStartUp() {
    qrm.GlobalConfig.JsonUnmarshalFunc = sonic.Unmarshal
}
```
