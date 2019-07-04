
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
* SQL Builder files - used to write type safe SQL statements in Go (`enum` and `table` package)
* Model files - used to store result from database queries (`model` package)
