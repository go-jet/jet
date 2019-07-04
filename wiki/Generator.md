
Before we can write SQL queries in Go we have to generate necessary Go files. 
To generate files we need running database instance containing already defined database schema. 

This files can be generated in two ways:

#### 1) Generating from command line

Install jetgen to GOPATH bin folder. This will allow generating jet files from the command line.

```sh
go install github.com/go-jet/jet/cmd/jetgen
```

Make sure GOPATH bin folder is added to the PATH environment variable.

Test jetgen can be found in the PATH.
```sh
jetgen -h
Usage of jetgen:
  -host string
    	Database host path (Example: localhost)
  -port string
    	Database port
  -user string
        Database user
  -password string
    	The user’s password
  -dbname string
    	name of the database
  -schema string
    	Database schema name. (default "public")
  -path string
    	Destination dir for generated files.
  -sslmode string
    	Whether or not to use SSL(optional) (default "disable")
  -params string
    	Additional connection string parameters(optional)
```

Now to generate sample database:
```sh
jetgen -host=localhost -port=5432 -user=jet -password=jet -dbname=jetdb -schema dvds -path ./gen
```
```sh
Connecting to postgres database: host=localhost port=5432 user=jet password=jet dbname=jetdb sslmode=disable 
Retrieving schema information...
	FOUND 15  table(s),  1  enum(s)
Cleaning up destination directory...
Generating table sql builder files...
Generating table model files...
Generating enum sql builder files...
Generating enum model files...
Done
```
#### 2) Generating from code

```
import "github.com/go-jet/jet/generator/postgresgen"

...

err = postgresgen.Generate("./gen", postgresgen.DBConnection{
    Host:       "localhost",
    Port:       "5432",
    User:       "jet",
    Password:   "jet",
    DBName:     "jetdb",
    SchemaName: "dvds",
    SslMode:    "disable",
})
```

In both ways, generator will:  
- connect to postgres database and retrieve information about the tables and enums of `dvds` schema
- delete everything in destination folder `./gen`,   
- generate sql builder and model Go files for each schema tables and enums into destination folder `./gen`.  

Generated files folder structure will look like this:
```
|-- gen                               # destination folder
|   `-- jetdb                         # database name
|       `-- dvds                      # schema name
|           |-- enum                  # sql builder folder for enums
|           |   |-- mpaa_rating.go
|           |-- table                 # sql builder folder for tables
|               |-- actor.go
|               |-- address.go
|               |-- film.go
                ...
|           |-- model                 # Plain Old Data for every enum and table
|           |   |-- actor.go
|           |   |-- address.go
|           |   |-- film.go
                ...

```

Table and enums from database schema are used as a template to generate two types of Go files:
* SQL builder files: Files used to write type safe SQL statements in Go (enum and table package)
* Model files: Files used to store result from database queries (model package)

#### SQL builder files
Part of the sample sql builder file for table `film`. 

```
package table

import (
	"github.com/go-jet/jet"
)
var Film = newFilmTable()

type FilmTable struct {
	jet.Table

	//Columns
	FilmID          jet.ColumnInteger
	Title           jet.ColumnString
	Description     jet.ColumnString
	ReleaseYear     jet.ColumnInteger
	LanguageID      jet.ColumnInteger
	RentalDuration  jet.ColumnInteger
	RentalRate      jet.ColumnFloat
	Length          jet.ColumnInteger
	ReplacementCost jet.ColumnFloat
	Rating          jet.ColumnString
	LastUpdate      jet.ColumnTimestamp
	SpecialFeatures jet.ColumnString
	Fulltext        jet.ColumnString

	AllColumns jet.ColumnList
}

// creates new FilmTable with assigned alias
func (a *FilmTable) AS(alias string) *FilmTable {
	aliasTable := newFilmTable()

	aliasTable.Table.AS(alias)

	return aliasTable
}
```
Table name and column names are camelized inside sql builder type.
`AllColumns` is used as shorthand notation for list of all columns `FilmID, Title, Description,...`.

Mappings of database types to sql builder column types are following:

| Database type(postgres)                         | Sql builder column type                                            |
| ----------------------------------------------- | -------------------------------------------------- |
| boolean                                         |  ColumnBool                                        |
| smallint, integer, bigint                       |  ColumnInteger                                     |
| real, numeric, decimal, double precision        |  ColumnFloat                                       |
| date                                            |  ColumnDate                                        |
| timestamp without time zone                     |  ColumnTimestamp                                   |
| timestamp with time zone                        |  ColumnTimestampz                                  |
| time without time zone                          |  ColumnTime                                        |
| time with time zone                             |  ColumnTimez                                       |
| enums, text, character, character varying       |                                                    |
| bytea, uuid                                     |                                                    |
| and all remaining types                         |  ColumnString                                      |

Sql builder file for enum type `mpaa_rating`:
```
package enum

import "github.com/go-jet/jet"

var MpaaRating = &struct {
	G    jet.StringExpression
	PG   jet.StringExpression
	PG13 jet.StringExpression
	R    jet.StringExpression
	NC17 jet.StringExpression
}{
	G:    jet.NewEnumValue("G"),
	PG:   jet.NewEnumValue("PG"),
	PG13: jet.NewEnumValue("PG-13"),
	R:    jet.NewEnumValue("R"),
	NC17: jet.NewEnumValue("NC-17"),
}

```
