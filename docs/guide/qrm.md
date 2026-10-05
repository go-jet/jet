# Query Result Mapping (QRM)

## How scan works

The `Query` and `QueryContext` statement methods scan and group each result row into an arbitrary destination
structure.

- `Query(db qrm.Queryable, destination interface{}) error` - executes the statement over database connection
  (or transaction) `db` and stores the result in `destination`.
- `QueryContext(ctx context.Context, db qrm.Queryable, destination interface{}) error` - same as `Query`, with
  a context.

The destination can be either a pointer to a struct or a pointer to a slice of structs.

The easiest way to understand scanning is by example. Let's retrieve a list of cities, the customers in each
city, and the address of each customer, limited to 'London' and 'York'.

```go
stmt := SELECT(
    City.CityID,
    City.City,
    Address.AddressID,
    Address.Address,
    Customer.CustomerID,
    Customer.LastName,
).FROM(
    City.
        INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
        INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)),
).WHERE(
    City.City.EQ(String("London")).OR(City.City.EQ(String("York"))),
).ORDER_BY(
    City.CityID, Address.AddressID, Customer.CustomerID,
)
```

Debug SQL of the statement:

```sql
SELECT city.city_id AS "city.city_id",
     city.city AS "city.city",
     address.address_id AS "address.address_id",
     address.address AS "address.address",
     customer.customer_id AS "customer.customer_id",
     customer.last_name AS "customer.last_name"
FROM dvds.city
     INNER JOIN dvds.address ON (address.city_id = city.city_id)
     INNER JOIN dvds.customer ON (customer.address_id = address.address_id)
WHERE (city.city = 'London') OR (city.city = 'York')
ORDER BY city.city_id, address.address_id, customer.customer_id;
```

**Every column is aliased by default. The format is `table_name.column_name`.**

The statement produces the following result set:

| _row_ | city.city_id | city.city | address.address_id | address.address      | customer.customer_id | customer.last_name |
| ----- | ------------ | --------- | ------------------ | -------------------- | -------------------- | ------------------ |
| _1_   | 312          | "London"  | 256                | "1497 Yuzhou Drive"  | 252                  | "Hoffman"          |
| _2_   | 312          | "London"  | 517                | "548 Uruapan Street" | 512                  | "Vines"            |
| _3_   | 589          | "York"    | 502                | "1515 Korla Way"     | 497                  | "Sledge"           |

Execute the statement and scan the result set into `dest`:

```go
var dest []struct {
    model.City

    Customers []struct {
        model.Customer

        Address model.Address
    }
}

err := stmt.Query(db, &dest)
```

The camel case of the result set column aliases matches `model type name`.`field name`. For instance
`city.city_id` maps to `City.CityID`. This is how the destination field for each column is found. It is not
an error if there is no column for a destination field. Table and column names do not have to be in snake case.

`Query` uses reflection to introspect the destination type and the result set column aliases. Every new
destination struct object is cached by its own primary key together with the primary keys of all its parents.
For grouping to work correctly, table primary keys have to appear in the result set. If there is no primary key
in the result set, the row number is used as the grouping key (which is always unique).

After row 1 is processed, two objects are stored in the cache:

```
Key:                                        Object:
(City(312))                              -> (*struct { model.City; Customers []struct { model.Customer; Address model.Address } })
(City(312)),(Customer(252),Address(256)) -> (*struct { model.Customer; Address model.Address })
```

After row 2, only one new object is stored, because the city with `city_id` 312 is already cached:

```
Key:                                        Object:
(City(312))                              -> pulled from cache
(City(312)),(Customer(512),Address(517)) -> (*struct { model.Customer; Address model.Address })
```

`dest` printed as JSON:

```json
[
    {
        "CityID": 312,
        "City": "London",
        "CountryID": 0,
        "LastUpdate": "0001-01-01T00:00:00Z",
        "Customers": [
            {
                "CustomerID": 252,
                "StoreID": 0,
                "FirstName": "",
                "LastName": "Hoffman",
                "Email": null,
                "AddressID": 0,
                "Activebool": false,
                "CreateDate": "0001-01-01T00:00:00Z",
                "LastUpdate": null,
                "Active": null,
                "Address": {
                    "AddressID": 256,
                    "Address": "1497 Yuzhou Drive",
                    "Address2": null,
                    "District": "",
                    "CityID": 0,
                    "PostalCode": null,
                    "Phone": "",
                    "LastUpdate": "0001-01-01T00:00:00Z"
                }
            },
            {
                "CustomerID": 512,
                "StoreID": 0,
                "FirstName": "",
                "LastName": "Vines",
                "Email": null,
                "AddressID": 0,
                "Activebool": false,
                "CreateDate": "0001-01-01T00:00:00Z",
                "LastUpdate": null,
                "Active": null,
                "Address": {
                    "AddressID": 517,
                    "Address": "548 Uruapan Street",
                    "Address2": null,
                    "District": "",
                    "CityID": 0,
                    "PostalCode": null,
                    "Phone": "",
                    "LastUpdate": "0001-01-01T00:00:00Z"
                }
            }
        ]
    },
    {
        "CityID": 589,
        "City": "York",
        "CountryID": 0,
        "LastUpdate": "0001-01-01T00:00:00Z",
        "Customers": [
            {
                "CustomerID": 497,
                "StoreID": 0,
                "FirstName": "",
                "LastName": "Sledge",
                "Email": null,
                "AddressID": 0,
                "Activebool": false,
                "CreateDate": "0001-01-01T00:00:00Z",
                "LastUpdate": null,
                "Active": null,
                "Address": {
                    "AddressID": 502,
                    "Address": "1515 Korla Way",
                    "Address2": null,
                    "District": "",
                    "CityID": 0,
                    "PostalCode": null,
                    "Phone": "",
                    "LastUpdate": "0001-01-01T00:00:00Z"
                }
            }
        ]
    }
]
```

All fields without a source column in the result set are initialized with their zero value. London has two
customers, which is the result of object reuse while processing row 2.

## Custom model types

Destinations are not limited to generated model types. Any destination works, as long as the projection alias
corresponds to `model type name`.`field name`. Only letters are compared, casing (lowercase, uppercase,
CamelCase, ...) is ignored.

**Go struct fields have to be exported for scan to work.**

A field type can be any base Go type, plus any type that implements the `sql.Scanner` interface (`uuid.UUID`,
`decimal.Decimal`, ...).

The example above, rewritten with custom model types:

```go
// Address has the same name and fields as the generated model struct
type Address struct {
    ID          int32 `sql:"primary_key"`
    AddressLine string
}

type MyCustomer struct {
    ID       int32 `sql:"primary_key"`
    LastName *string

    Address Address
}

type MyCity struct {
    ID   int32 `sql:"primary_key"`
    Name string

    Customers []MyCustomer
}

dest2 := []MyCity{}

stmt2 := SELECT(
    City.CityID.AS("my_city.id"),                  // snake case
    City.City.AS("myCity.Name"),                   // camel case
    Address.AddressID,                             // no need for aliasing
    Address.Address,                               // default aliasing still works
    Customer.CustomerID.AS("My_Customer.id"),      // mixed case
    Customer.LastName.AS("my customer.last name"), // with spaces
).FROM(
    City.
        INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
        INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)),
).WHERE(
    City.City.EQ(String("London")).OR(City.City.EQ(String("York"))),
).ORDER_BY(
    City.CityID, Address.AddressID, Customer.CustomerID,
)

err := stmt2.Query(db, &dest2)
```

Destination type names and field names have changed: every type has a `My` prefix, every primary key is named
`ID`, `LastName` is now a string pointer, and so on. Because the custom types use different identifiers, each
column now needs an alias for the mapping to work. For instance `City.CityID.AS("my_city.id")` maps to
`MyCity.ID`, `City.City.AS("myCity.Name")` maps to `MyCity.Name`, etc.

::: tip
Table names, column names and aliases do not have to be snake case. CamelCase, PascalCase or other mixed
casings are supported, but snake case is strongly recommended for database identifiers.
:::

JSON of the new destination:

```json
[
    {
        "ID": 312,
        "Name": "London",
        "Customers": [
            {
                "ID": 252,
                "LastName": "Hoffman",
                "Address": {
                    "ID": 256,
                    "AddressLine": "1497 Yuzhou Drive"
                }
            },
            {
                "ID": 512,
                "LastName": "Vines",
                "Address": {
                    "ID": 517,
                    "AddressLine": "548 Uruapan Street"
                }
            }
        ]
    },
    {
        "ID": 589,
        "Name": "York",
        "Customers": [
            {
                "ID": 497,
                "LastName": "Sledge",
                "Address": {
                    "ID": 502,
                    "AddressLine": "1515 Korla Way"
                }
            }
        ]
    }
]
```

### Anonymous custom types

There is no need to create a named type every time. The destination type can be declared inline:

```go
var dest []struct {
    CityID   int32 `sql:"primary_key"`
    CityName string

    Customers []struct {
        CustomerID int32 `sql:"primary_key"`
        LastName   string

        Address struct {
            AddressID   int32 `sql:"primary_key"`
            AddressLine string
        }
    }
}

stmt := SELECT(
    City.CityID.AS("city_id"),
    City.City.AS("city_name"),
    Customer.CustomerID.AS("customer_id"),
    Customer.LastName.AS("last_name"),
    Address.AddressID.AS("address_id"),
    Address.Address.AS("address_line"),
).FROM(
    City.
        INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
        INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)),
).WHERE(
    City.City.EQ(String("London")).OR(City.City.EQ(String("York"))),
).ORDER_BY(
    City.CityID, Address.AddressID, Customer.CustomerID,
)

err := stmt.Query(db, &dest)
```

Aliasing is simplified: an alias contains only the column/field name. On the other hand, three fields named
`ID` are not possible, because aliases must be unique.

### Tagging model type fields

The mapping can also be set the other way around, by tagging destination fields and types:

```go
var dest []struct {
    CityID   int32  `sql:"primary_key" alias:"city.city_id"`
    CityName string `alias:"city.city"`

    Customers []struct {
        // because the whole struct refers to 'customer.*' (see tag below),
        // 'alias:"customer_id"' can be used instead of 'alias:"customer.customer_id"'
        CustomerID int32   `sql:"primary_key" alias:"customer_id"`
        LastName   *string `alias:"last_name"`

        Address struct {
            AddressID   int32  `sql:"primary_key" alias:"AddressId"` // camel case alias works as well
            AddressLine string `alias:"address.address"`            // full alias works as well
        } `alias:"address.*"`                                       // struct refers to all address.* columns

    } `alias:"customer.*"`                                          // struct refers to all customer.* columns
}

stmt := SELECT(
    City.CityID,
    City.City,
    Customer.CustomerID,
    Customer.LastName,
    Address.AddressID,
    Address.Address,
).FROM(
    City.
        INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
        INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)),
).WHERE(
    City.City.EQ(String("London")).OR(City.City.EQ(String("York"))),
).ORDER_BY(
    City.CityID, Address.AddressID, Customer.CustomerID,
)

err := stmt.Query(db, &dest)
```

This kind of mapping is more involved than the previous examples and should be reserved for cases with no
alternative. Usually that is one of two scenarios:

#### Self join

```go
var dest []struct {
    model.Employee

    Manager *model.Employee `alias:"Manager.*"` // or just `alias:"Manager"`
}

manager := Employee.AS("Manager")

stmt := SELECT(
    Employee.EmployeeId,
    Employee.FirstName,
    manager.EmployeeId,
    manager.FirstName,
).FROM(
    Employee.
        LEFT_JOIN(manager, Employee.ReportsTo.EQ(manager.EmployeeId)),
)
```

_This example could also be written without an alias tag, by introducing a new type `type Manager model.Employee`._

#### Slices of Go base types

```go
var dest struct {
    model.Film

    InventoryIDs []int32 `alias:"inventory.inventory_id"`
}
```

## Combining generated and custom model types

Generated and custom model types can be freely combined:

```go
type MyCustomer struct {
    ID       int32 `sql:"primary_key"`
    LastName string

    Address model.Address // model.Address is a generated model type
}

type MyCity struct {
    ID   int32 `sql:"primary_key"`
    Name string

    Customers []MyCustomer
}
```

## Specifying primary keys

Model types generated from database views do not contain any field tagged `primary_key`, so they cannot be used
as a grouping destination as-is:

```go
var dest []struct {
    model.ActorInfo // <- view model, without `primary_key` fields
    Films []model.Film
}
```

Querying into this destination would not give the correct result, because the `Films` slice does not know which
`ActorInfo` it belongs to. To fix it, specify the primary keys for the view model type manually:

```go
var dest []struct {                             // ID is a field name in model.ActorInfo
    model.ActorInfo `sql:"primary_key=ID"`      // comma separated list of field names
    Films []model.Film
}
```

The same tag can be used to override primary key fields on a model type that already has them.

## Strict scan

Since `v2.13.0`, QRM can perform a strict scan during result mapping. When enabled, `Query` panics if any
column in the SQL result set is not mapped to a field in the destination struct. This catches mismatches early
and reduces silent bugs caused by unused columns.

Configure strict scan during application initialization:

```go
import (
    "github.com/go-jet/jet/v2/qrm"
)

func ServerStartUp() {
    qrm.GlobalConfig.StrictScan = true
}
```

::: info
Strict scan does not apply to statements built with `SELECT_JSON_OBJ` or `SELECT_JSON_ARR`.
:::
