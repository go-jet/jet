# Model

Model files contain plain Go struct types used to store the results of SQL queries. Model types can be used
alone or combined into nested structures. They are auto-generated from database tables, views
and enums.

## Table and view model files

The following rules are applied when generating model types from tables and views:

- For every table there is one Go file generated. The file name is the snake case of the table name.
- Every model file contains one struct type. The type name is the camel case of the table name. The package
  name is always `model`.
- For every column there is a field in the model struct. The field name is the camel case of the column name.
  See the tables below for type mapping.
- Fields are pointer types if the column can be `NULL`.
- Fields corresponding to primary key columns are tagged with `sql:"primary_key"`. This tag is used during
  query execution to group row results into the destination structure. See
  [Query Result Mapping (QRM)](./qrm).

### PostgreSQL type mapping

| Database type (PostgreSQL)                                     | Go type     |
| -------------------------------------------------------------- | ----------- |
| boolean                                                        | `bool`      |
| smallint                                                       | `int16`     |
| integer                                                        | `int32`     |
| bigint                                                         | `int64`     |
| real                                                           | `float32`   |
| numeric, decimal, double precision                             | `float64`   |
| date, timestamp, time (with or without time zone)              | `time.Time` |
| bytea                                                          | `[]byte`    |
| uuid                                                           | `uuid.UUID` |
| enum                                                           | enum name   |
| text, character, character varying and all remaining types     | `string`    |

### MySQL and MariaDB type mapping

| Database type (MySQL / MariaDB)                                | Go type                 |
| ------------------------------------------------------------- | ----------------------- |
| boolean or BIT(1)                                             | `bool`                  |
| tinyint [unsigned]                                            | `[u]int8`               |
| smallint [unsigned]                                           | `[u]int16`              |
| mediumint [unsigned]                                          | `[u]int32`              |
| integer [unsigned]                                            | `[u]int32`              |
| bigint [unsigned]                                             | `[u]int64`              |
| real                                                          | `float32`               |
| numeric, decimal, double precision                            | `float64`               |
| date, time, datetime, timestamp                               | `time.Time`             |
| binary, varbinary, tinyblob, blob, mediumblob, longblob       | `[]byte`                |
| enum                                                          | table name + enum name  |
| text, character, character varying and all remaining types    | `string`                |

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

Generated model file `address.go`:

```go
package model

import (
    "time"
)

type Address struct {
    AddressID  int32 `sql:"primary_key"`
    Address    string
    Address2   *string
    District   string
    CityID     int16
    PostalCode *string
    Phone      string
    LastUpdate time.Time
}
```

## Enum model files

The following rules are applied when generating model files from enums:

- For every enum there is one Go file generated.
  - PostgreSQL: the file name is the snake case of the enum name.
  - MySQL or MariaDB: the file name is the snake case of `table name` + `enum name`.
- Every file contains one named string type. The type name is the camel case of the enum name. The package
  name is always `model`. The enum type has two helper methods: `Scan`, which initializes the value from a query result, and
  `String`.
- For every enum value there is one constant defined, named `{CamelCase(enum_name)}_{CamelCase(enum_value_name)}`.
- There is one slice containing all enum values, named `{CamelCase(enum_name)}AllValues`.

### Example

PostgreSQL:

```sql
CREATE TYPE dvds.mpaa_rating AS ENUM
    ('G', 'PG', 'PG-13', 'R', 'NC-17');
```

Generated model file `mpaa_rating.go`:

```go
package model

import "errors"

type MpaaRating string

const (
	MpaaRating_G    MpaaRating = "G"
	MpaaRating_Pg   MpaaRating = "PG"
	MpaaRating_Pg13 MpaaRating = "PG-13"
	MpaaRating_R    MpaaRating = "R"
	MpaaRating_Nc17 MpaaRating = "NC-17"
)

var MpaaRatingAllValues = []MpaaRating{
	MpaaRating_G,
	MpaaRating_Pg,
	MpaaRating_Pg13,
	MpaaRating_R,
	MpaaRating_Nc17,
}

func (e *MpaaRating) Scan(value interface{}) error {
	var enumValue string
	switch val := value.(type) {
	case string:
		enumValue = val
	case []byte:
		enumValue = string(val)
	default:
		return errors.New("jet: Invalid scan value for MpaaRating enum. Enum value has to be of type string or []byte")
	}

	switch enumValue {
	case "G":
		*e = MpaaRating_G
	case "PG":
		*e = MpaaRating_Pg
	case "PG-13":
		*e = MpaaRating_Pg13
	case "R":
		*e = MpaaRating_R
	case "NC-17":
		*e = MpaaRating_Nc17
	default:
		return errors.New("jet: Invalid scan value '" + enumValue + "' for MpaaRating enum")
	}

	return nil
}

func (e MpaaRating) String() string {
	return string(e)
}
```

MySQL or MariaDB:

```sql
CREATE TABLE film (
  rating ENUM('G','PG','PG-13','R','NC-17') DEFAULT 'G'
)
```

Generated model file `film_rating.go`:

```go
package model

import "errors"

type FilmRating string

const (
	FilmRating_G    FilmRating = "G"
	FilmRating_Pg   FilmRating = "PG"
	FilmRating_Pg13 FilmRating = "PG-13"
	FilmRating_R    FilmRating = "R"
	FilmRating_Nc17 FilmRating = "NC-17"
)

var FilmRatingAllValues = []FilmRating{
	FilmRating_G,
	FilmRating_Pg,
	FilmRating_Pg13,
	FilmRating_R,
	FilmRating_Nc17,
}

func (e *FilmRating) Scan(value interface{}) error {
	var enumValue string
	switch val := value.(type) {
	case string:
		enumValue = val
	case []byte:
		enumValue = string(val)
	default:
		return errors.New("jet: Invalid scan value for FilmRating enum. Enum value has to be of type string or []byte")
	}

	switch enumValue {
	case "G":
		*e = FilmRating_G
	case "PG":
		*e = FilmRating_Pg
	case "PG-13":
		*e = FilmRating_Pg13
	case "R":
		*e = FilmRating_R
	case "NC-17":
		*e = FilmRating_Nc17
	default:
		return errors.New("jet: Invalid scan value '" + enumValue + "' for FilmRating enum")
	}

	return nil
}

func (e FilmRating) String() string {
	return string(e)
}
```
