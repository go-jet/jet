# Generator

Before writing SQL queries in Go, the SQL Builder and Model files have to be generated. Generation requires a
running database instance with the schema already defined.

Files can be generated from the command line or programmatically from Go code.

## Generating from the command line

Install `jet` into one of the folders on your `PATH`, so files can be generated from any location in the shell.

- Using `go install`:

  ```sh
  go install github.com/go-jet/jet/v2/cmd/jet@latest
  ```

  The generator is installed to the directory named by the `GOBIN` environment variable, which defaults to
  `$GOPATH/bin` or `$HOME/go/bin` if `GOPATH` is not set.

- Build into a specific folder:

  ```sh
  git clone https://github.com/go-jet/jet.git
  cd jet && go build -o dir_path ./cmd/jet
  ```

  Make sure `dir_path` is added to the `PATH` environment variable.

Check that the generator is on the `PATH`:

```
$ jet -h

Jet generator v2.16.0

Usage:
  -source
        Database system name (postgres, mysql, cockroachdb, mariadb or sqlite)
  -dsn
        Data source name. Unified format for connecting to database.
        PostgreSQL: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
                Example:
                        postgresql://user:pass@localhost:5432/dbname
        MySQL: https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html
                Example:
                        mysql://jet:jet@tcp(localhost:3306)/dvds
        SQLite: https://www.sqlite.org/c3ref/open.html#urifilenameexamples
                Example:
                        file://path/to/database/file
  -host
        Database host path. Used only if dsn is not set. (Example: localhost)
  -port
        Database port. Used only if dsn is not set.
  -user
        Database user. Used only if dsn is not set.
  -password
        The user's password. Used only if dsn is not set.
  -dbname
        Database name. Used only if dsn is not set.
  -schema
        Database schema name. (default "public")(PostgreSQL only)
  -params
        Additional connection string parameters(optional). Used only if dsn is not set.
  -sslmode
        Whether or not to use SSL. Used only if dsn is not set. (optional)(default "disable")(PostgreSQL only)
  -path
        Destination directory for files generated.
  -ignore-tables
        Comma-separated list of tables to ignore. Names may use shell wildcards, e.g. "user_*".
  -ignore-views
        Comma-separated list of views to ignore. Names may use shell wildcards, e.g. "user_*".
  -ignore-enums
        Comma-separated list of enums to ignore. Names may use shell wildcards, e.g. "user_*".
  -skip-model
        Skip model generation.
  -skip-sql-builder
        Skip SQL builder generation.
  -rel-model-path
        Relative path for the Model files package from the destination directory.
  -rel-table-path
        Relative path for the Table files package from the destination directory.
  -rel-view-path
        Relative path for the View files package from the destination directory.
  -rel-enum-path
        Relative path for the Enum files package from the destination directory.
  -tables
        Comma-separated list of tables to generate. Names may use shell wildcards, e.g. "user_*".
  -views
        Comma-separated list of views to generate. Names may use shell wildcards, e.g. "user_*".
  -enums
        Comma-separated list of enums to generate. Names may use shell wildcards, e.g. "user_*".
  -version
        Print version.

Example commands:

        $ jet -dsn=postgresql://jet:jet@localhost:5432/jetdb?sslmode=disable -schema=dvds -path=./gen
        $ jet -dsn=postgres://jet:jet@localhost:26257/jetdb?sslmode=disable -schema=dvds -path=./gen   #cockroachdb
        $ jet -source=postgres -dsn="user=jet password=jet host=localhost port=5432 dbname=jetdb" -schema=dvds -path=./gen
        $ jet -source=mysql -host=localhost -port=3306 -user=jet -password=jet -dbname=jetdb -path=./gen
        $ jet -source=sqlite -dsn="file://path/to/sqlite/database/file" -path=./gen
        $ jet -source=sqlite -dsn="file://path/to/sqlite/database/file" -path=./gen -rel-model-path=./entity
```

### PostgreSQL or CockroachDB

To generate SQL Builder and Model files from a PostgreSQL (or CockroachDB) database, call the generator with
the connection parameters and the root destination folder for generated files.

Assuming a local database with user `jet`, password `pass`, database `jetdb` and schema `dvds`:

```sh
jet -dsn=postgresql://jet:pass@localhost:5432/jetdb -schema=dvds -path=./.gen
```

or

```sh
jet -source=postgres -host=localhost -port=5432 -user=jet -password=pass -dbname=jetdb -schema=dvds -path=./gen
```

```
Connecting to postgres database: host=localhost port=5432 user=jet password=pass dbname=jetdb sslmode=disable
Retrieving schema information...
	FOUND 15 table(s), 7 view(s), 1 enum(s)
Destination directory: ./gen/jetdb/dvds
Cleaning up destination directory...
Generating table sql builder files...
Generating view sql builder files...
Generating enum sql builder files...
Generating table model files...
Generating view model files...
Generating enum model files...
Done
```

### MySQL or MariaDB

Same as above, with MySQL connection parameters and no schema flag:

```sh
jet -dsn="mariadb://jet:pass@tcp(localhost:3306)/dvds" -path=./gen
```

or

```sh
jet -source=mysql -host=localhost -port=3306 -user=jet -password=pass -dbname=dvds -path=./gen
```

### SQLite

For SQLite only the database file path and the destination folder are needed:

```sh
jet -dsn="file:///path/to/sqlite/database/file" -path=./gen
```

## Generating from code

The same files can be generated programmatically.

::: code-group

```go [PostgreSQL]
import "github.com/go-jet/jet/v2/generator/postgres"

...

err = postgres.Generate("./gen", // or GenerateDSN(...)
	postgres.DBConnection{
		Host:       "localhost",
		Port:       5432,
		User:       "jet",
		Password:   "jet",
		DBName:     "jetdb",
		SchemaName: "dvds",
		SslMode:    "disable",
	})
```

```go [MySQL / MariaDB]
import "github.com/go-jet/jet/v2/generator/mysql"

...

err = mysql.Generate("./.gen", // or GenerateDSN(...)
	mysql.DBConnection{
		Host:     "localhost",
		Port:     3306,
		User:     "jet",
		Password: "jet",
		DBName:   "jetdb",
	})
```

:::

Whether run from the command line or from code, the generator will:

- connect to the database and retrieve information about _tables_, _views_ and _enums_
- delete everything in the destination folder
- use table, view and enum information as a template to generate two kinds of Go files:
  - SQL Builder files - used to write type-safe SQL statements in Go (`table`, `view` and `enum` packages)
  - Model files - used to combine and store results of database queries (`model` package)

Generated folder structure:

::: code-group

```txt [PostgreSQL]
|-- gen                               # -path
|   `-- jetdb                         # database name
|       `-- dvds                      # schema name
|           |-- enum                  # sql builder package for enums
|           |   |-- mpaa_rating.go
|           |-- table                 # sql builder package for tables
|           |   |-- actor.go
|           |   |-- address.go
|           |   |-- category.go
|           |   ...
|           |-- view                  # sql builder package for views
|           |   |-- actor_info.go
|           |   |-- film_list.go
|           |   ...
|           |-- model                 # data model types for each table, view and enum
|           |   |-- actor.go
|           |   |-- address.go
|           |   |-- mpaa_rating.go
|           |   ...
```

```txt [MySQL / MariaDB]
|-- gen                           # -path
|   `-- jetdb                     # database name
|       |-- enum                  # sql builder package for enums
|       |   |-- film_rating.go
|       |-- table                 # sql builder package for tables
|       |   |-- actor.go
|       |   |-- address.go
|       |   |-- film.go
|       |   ...
|       |-- view                  # sql builder package for views
|       |   |-- actor_info.go
|       |   |-- film_list.go
|       |   ...
|       |-- model                 # data model types for each table, view and enum
|       |   |-- actor.go
|       |   |-- address.go
|       |   |-- film.go
|       |   ...
```

:::

## Generator customization

All aspects of the generated Model and SQL Builder files can be customized.

A common need is to change the type of a specific model field. In this example the exact decimal type
`decimal.Decimal` is used instead of the default `float64`, to prevent loss of precision:

```go
import (
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/generator/template"
	postgres2 "github.com/go-jet/jet/v2/postgres"
	"github.com/shopspring/decimal"
)

err := postgres.Generate( // or GenerateDSN(...)
	"./gen/dest/dir",
	dbConnection,
	template.Default(postgres2.Dialect).
		UseSchema(func(schema metadata.Schema) template.Schema {
			return template.DefaultSchema(schema).
				UseModel(template.DefaultModel().
					UseTable(func(table metadata.Table) template.TableModel {
						return template.DefaultTableModel(table).
							UseField(func(column metadata.Column) template.TableModelField {
								defaultTableModelField := template.DefaultTableModelField(column)

								if schema.Name == "public" &&
									table.Name == "accounts" &&
									column.Name == "balance" {
									defaultTableModelField.Type = template.NewType(decimal.Decimal{})
								}
								return defaultTableModelField
							})
					}),
				)
		}),
)
```

It is also possible to:

- [skip model file generation](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L56)
- [skip sql builder file generation](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L74)
- [change models destination path](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L94)
- [change sql builder destination path](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L112)
- [rename model files](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L135)
- [rename sql builder files](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L201)
- [skip specific model files](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L173)
- [skip specific sql builder files](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L201)
- [add tags to fields of the model types](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L274)
- [change type of a model field](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L316)
- [change type of a sql builder column](https://github.com/go-jet/jet/blob/f2e4b8551c48b97d0cd2d3deff47dc1b2aa2f04e/tests/postgres/generator_template_test.go#L359)

### Generic types in model fields

When a generic type's parameter is a standard Go type, `template.NewType` can be used to customize the model
field type:

```go
switch defaultTableModelField.Type.Name {
case "*string":
	defaultTableModelField.Type = template.NewType(sql.Null[string]{})
}
```

If the type parameter is not a standard Go type, construct the `Type` struct manually so the additional import
is emitted:

```go
switch defaultTableModelField.Type.Name {
case "*uuid.UUID":
	defaultTableModelField.Type = template.Type{
		ImportPath:            "database/sql",
		Name:                  "sql.Null[uuid.UUID]",
		AdditionalImportPaths: []string{"github.com/google/uuid"},
	}
}
```
