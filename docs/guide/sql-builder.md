# SQL Builder

SQL Builder files are Go files that contain the types needed to write secure, type-safe SQL queries. They are
generated automatically from information about database tables, views and enums.

## Table and view SQL Builder files

The following rules are applied when generating table and view SQL Builder files:

- For every table or view there is one Go file generated. The file name is the snake case of the table or view
  name.
- Every file contains one struct type with a nested `Table`.
- For every column there is a column field in the table type. The field name is the camel case of the column
  name. See the tables below for type mapping.
- `AllColumns` is a shorthand for the list of all columns.
- `MutableColumns` are all columns except primary key and generated columns. _Useful in INSERT or UPDATE
  statements._
- `DefaultColumns` are all columns with a `DEFAULT` value set.

### PostgreSQL column type mapping

| Database type (PostgreSQL)                                          | SQL builder column type |
| ------------------------------------------------------------------- | ----------------------- |
| boolean                                                             | `ColumnBool`            |
| smallint, integer, bigint                                           | `ColumnInteger`         |
| real, numeric, decimal, double precision                            | `ColumnFloat`           |
| date                                                                | `ColumnDate`            |
| timestamp without time zone                                         | `ColumnTimestamp`       |
| timestamp with time zone                                            | `ColumnTimestampz`      |
| time without time zone                                              | `ColumnTime`            |
| time with time zone                                                 | `ColumnTimez`           |
| bytea                                                               | `ColumnBytea`           |
| enums, text, character, character varying, uuid and remaining types | `ColumnString`          |

### MySQL and MariaDB column type mapping

| Database type (MySQL / MariaDB)                               | SQL builder column type |
| ------------------------------------------------------------- | ----------------------- |
| boolean                                                       | `ColumnBool`            |
| tinyint, smallint, mediumint, integer, bigint                 | `ColumnInteger`         |
| real, numeric, decimal, double precision                      | `ColumnFloat`           |
| date                                                          | `ColumnDate`            |
| timestamp, datetime                                           | `ColumnTimestamp`       |
| time                                                          | `ColumnTime`            |
| blob                                                          | `ColumnBlob`            |
| enums, text, character, character varying and remaining types | `ColumnString`          |

### Example

PostgreSQL table `address`:

```sql
CREATE TABLE dvds.address
(
    address_id serial NOT NULL DEFAULT,
    address character varying(50) NOT NULL,
    address2 character varying(50),
    district character varying(20) NOT NULL,
    city_id smallint NOT NULL,
    postal_code character varying(10),
    phone character varying(20) NOT NULL,
    last_update timestamp without time zone NOT NULL DEFAULT now(),
    CONSTRAINT address_pkey PRIMARY KEY (address_id)
)
```

Part of the generated SQL Builder file for table `address`:

```go
package table

import (
	"github.com/go-jet/jet/v2/postgres"
)

var Address = newAddressTable("dvds", "address", "")

type addressTable struct {
	postgres.Table

	// Columns
	AddressID  postgres.ColumnInteger
	Address    postgres.ColumnString
	Address2   postgres.ColumnString
	District   postgres.ColumnString
	CityID     postgres.ColumnInteger
	PostalCode postgres.ColumnString
	Phone      postgres.ColumnString
	LastUpdate postgres.ColumnTimestamp

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
	DefaultColumns postgres.ColumnList
}
```

## Enum SQL Builder files

The following rules are applied when generating enum SQL Builder files:

- For every enum there is one Go file generated.
  - PostgreSQL: the file name is the snake case of the enum name.
  - MySQL or MariaDB: the file name is the snake case of `table/view name` + `enum name`.
- Every file contains one type.
- For every enum value there is a field in the enum struct. The field name is the camel case of the enum
  value. The type is `StringExpression`, so it can be used with string expression methods.

### Example

PostgreSQL enum `mpaa_rating`:

```sql
CREATE TYPE dvds.mpaa_rating AS ENUM
    ('G', 'PG', 'PG-13', 'R', 'NC-17');
```

Enum SQL Builder file for `mpaa_rating`:

```go
package enum

import "github.com/go-jet/jet/v2/postgres"

var MpaaRating = &struct {
	G    postgres.StringExpression
	PG   postgres.StringExpression
	PG13 postgres.StringExpression
	R    postgres.StringExpression
	NC17 postgres.StringExpression
}{
	G:    postgres.NewEnumValue("G"),
	PG:   postgres.NewEnumValue("PG"),
	PG13: postgres.NewEnumValue("PG-13"),
	R:    postgres.NewEnumValue("R"),
	NC17: postgres.NewEnumValue("NC-17"),
}
```

This enum can be used in expressions like:

```go
Film.Rating.NOT_EQ(enum.MpaaRating.R)
```

## Column list

In addition to the generated `AllColumns`, `MutableColumns` and `DefaultColumns` lists, you can create a new
`ColumnList`:

```go
updateColumnList := ColumnList{Link.Description, Link.Name, Link.URL}
```

A new `ColumnList` can also be created by excluding columns from an existing list:

```go
Address.AllColumns.Except(Address.LastUpdate)
Address.AllColumns.Except(Address.PostalCode, Address.Phone, Address.LastUpdate)
Address.AllColumns.Except(StringColumn("postal_code"), StringColumn("phone"), TimestampColumn("last_update"))

excludedColumns := ColumnList{Address.PostalCode, Address.Phone, Address.LastUpdate, Film.Title}
Address.AllColumns.Except(excludedColumns)
```

To change the table alias of each column within a `ColumnList`, use the `As` method:

```go
Address.AllColumns.As("my_address.*")

Address.AllColumns.As("") // removes the table alias
```
