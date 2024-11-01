# Jet

[![go-jet](https://circleci.com/gh/go-jet/jet.svg?style=svg)](https://app.circleci.com/pipelines/github/go-jet/jet?branch=master)
[![codecov](https://codecov.io/gh/go-jet/jet/branch/master/graph/badge.svg)](https://codecov.io/gh/go-jet/jet)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-jet/jet)](https://goreportcard.com/report/github.com/go-jet/jet/v2)
[![Documentation](https://godoc.org/github.com/go-jet/jet?status.svg)](http://godoc.org/github.com/go-jet/jet/v2)
[![GitHub release](https://img.shields.io/github/release/go-jet/jet.svg)](https://github.com/go-jet/jet/releases)

Jet is a complete solution for efficient and high performance database access, consisting of type-safe SQL builder 
with code generation and automatic query result data mapping.  
Jet currently supports `PostgreSQL`, `MySQL`, `CockroachDB`, `MariaDB` and `SQLite`. Future releases will add support for additional databases.

![jet](https://github.com/go-jet/jet/wiki/image/jet.png)  
Jet is the easiest, and the fastest way to write complex type-safe SQL queries as a Go code and map database query result 
into complex object composition. __It is not an ORM.__ 

## Motivation
https://medium.com/@go.jet/jet-5f3667efa0cc

## Contents
- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Quick Start](#quick-start)
     - [Generate sql builder and model types](#generate-sql-builder-and-model-types)
     - [Lets write some SQL queries in Go](#lets-write-some-sql-queries-in-go)
     - [Execute query and store result](#execute-query-and-store-result)
- [Benefits](#benefits)
- [Dependencies](#dependencies)
- [Versioning](#versioning)
- [License](#license)

## Features
 1) Auto-generated type-safe SQL Builder. Statements supported:
    * [SELECT](https://github.com/go-jet/jet/wiki/SELECT) `(DISTINCT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, FOR, LOCK_IN_SHARE_MODE, UNION, INTERSECT, EXCEPT, WINDOW, sub-queries)`
    * [INSERT](https://github.com/go-jet/jet/wiki/INSERT) `(VALUES, MODEL, MODELS, QUERY, ON_CONFLICT/ON_DUPLICATE_KEY_UPDATE, RETURNING)`, 
    * [UPDATE](https://github.com/go-jet/jet/wiki/UPDATE) `(SET, MODEL, WHERE, RETURNING)`, 
    * [DELETE](https://github.com/go-jet/jet/wiki/DELETE) `(WHERE, ORDER_BY, LIMIT, RETURNING)`,
    * [LOCK](https://github.com/go-jet/jet/wiki/LOCK) `(IN, NOWAIT)`, `(READ, WRITE)`
    * [WITH](https://github.com/go-jet/jet/wiki/WITH)
    
 2) Auto-generated Data Model types - Go types mapped to database type (table, view or enum), used to store
 result of database queries. Can be combined to create complex query result destination. 
 3) Query execution with result mapping to arbitrary destination. 

## Getting Started

### Prerequisites

To install Jet package, you need to install Go and set your Go workspace first.

[Go](https://golang.org/) **version 1.18+ is required**

### Installation

Use the command bellow to add jet as a dependency into `go.mod` project:
```sh
$ go get -u github.com/go-jet/jet/v2
```

Jet generator can be installed in one of the following ways:

- (Go1.16+) Install jet generator using go install:
```sh
go install github.com/go-jet/jet/v2/cmd/jet@latest
```
*Jet generator is installed to the directory named by the GOBIN environment variable,
which defaults to $GOPATH/bin or $HOME/go/bin if the GOPATH environment variable is not set.*

- Install jet generator to specific folder:
```sh
git clone https://github.com/go-jet/jet.git
cd jet && go build -o dir_path ./cmd/jet
```
*Make sure `dir_path` folder is added to the PATH environment variable.*



### Quick Start
For this quick start example we will use PostgreSQL sample _'dvd rental'_ database. Full database dump can be found in
[./tests/testdata/init/postgres/dvds.sql](https://github.com/go-jet/jet-test-data/blob/master/init/postgres/dvds.sql).
Schema diagram of interest can be found [here](./examples/quick-start/diagram.png).

#### Generate SQL Builder and Model types
To generate jet SQL Builder and Data Model types from running postgres database, we need to call `jet` generator with postgres 
connection parameters and destination folder path. 
Assuming we are running local postgres database, with user `user`, user password `pass`, database `jetdb` and 
schema `dvds` we will use this command:
```sh
jet -dsn=postgresql://user:pass@localhost:5432/jetdb?sslmode=disable -schema=dvds -path=./.gen
```
```sh
Connecting to postgres database: postgresql://user:pass@localhost:5432/jetdb?sslmode=disable 
Retrieving schema information...
	FOUND 15 table(s), 7 view(s), 1 enum(s)
Cleaning up destination directory...
Generating table sql builder files...
Generating view sql builder files...
Generating enum sql builder files...
Generating table model files...
Generating view model files...
Generating enum model files...
Done
```
Procedure is similar for MySQL, CockroachDB, MariaDB and SQLite. For example:
```sh
jet -source=mysql -dsn="user:pass@tcp(localhost:3306)/dbname" -path=./.gen
jet -dsn=postgres://user:pass@localhost:26257/jetdb?sslmode=disable -schema=dvds -path=./.gen  #cockroachdb
jet -dsn="mariadb://user:pass@tcp(localhost:3306)/dvds" -path=./.gen              # source flag can be omitted if data source appears in dsn
jet -source=sqlite -dsn="/path/to/sqlite/database/file" -schema=dvds -path=./.gen
jet -dsn="file:///path/to/sqlite/database/file" -schema=dvds -path=./.gen         # sqlite database assumed for 'file' data sources
```
_*User has to have a permission to read information schema tables._

As command output suggest, Jet will:
- connect to postgres database and retrieve information about the _tables_, _views_ and _enums_ of `dvds` schema
- delete everything in schema destination folder -  `./.gen/jetdb/dvds`,   
- and finally generate SQL Builder and Model types for each schema table, view and enum.  


Generated files folder structure will look like this:
```sh 
|-- .gen                              # path
|   -- jetdb                          # database name
|       -- dvds                       # schema name
|           |-- enum                  # sql builder package for enums
|           |   |-- mpaa_rating.go
|           |-- table                 # sql builder package for tables
|               |-- actor.go
|               |-- address.go
|               |-- category.go
|               ...
|           |-- view                  # sql builder package for views
|               |-- actor_info.go
|               |-- film_list.go
|               ...
|           |-- model                 # data model types for each table, view and enum
|           |   |-- actor.go
|           |   |-- address.go
|           |   |-- mpaa_rating.go
|           |   ...
```
Types from `table`, `view` and `enum` are used to write type safe SQL in Go, and `model` types are combined to store 
results of the SQL queries.



#### Let's write some SQL queries in Go

First we need to import postgres SQLBuilder and generated packages from the previous step:
```go
import (
	// dot import so go code would resemble as much as native SQL
	// dot import is not mandatory
	. "github.com/go-jet/jet/v2/examples/quick-start/.gen/jetdb/dvds/table"
	. "github.com/go-jet/jet/v2/postgres"

	"github.com/go-jet/jet/v2/examples/quick-start/.gen/jetdb/dvds/model"
)
```
Let's say we want to retrieve the list of all _actors_ that acted in _films_ longer than 180 minutes, _film language_ is 'English' 
and _film category_ is not 'Action'.  
```golang
stmt := SELECT(
    Actor.ActorID, Actor.FirstName, Actor.LastName, Actor.LastUpdate,  // or just Actor.AllColumns
    Film.AllColumns,                                                  
    Language.AllColumns.Except(Language.LastUpdate),  // all language columns except last_update 
    Category.AllColumns,
).FROM(
    Actor.
        INNER_JOIN(FilmActor, Actor.ActorID.EQ(FilmActor.ActorID)).  
        INNER_JOIN(Film, Film.FilmID.EQ(FilmActor.FilmID)).          
        INNER_JOIN(Language, Language.LanguageID.EQ(Film.LanguageID)).
        INNER_JOIN(FilmCategory, FilmCategory.FilmID.EQ(Film.FilmID)).
        INNER_JOIN(Category, Category.CategoryID.EQ(FilmCategory.CategoryID)),
).WHERE(
    Language.Name.EQ(Char(20)("English")).             
        AND(Category.Name.NOT_EQ(Text("Action"))).  
        AND(Film.Length.GT(Int32(180))),               
).ORDER_BY(
    Actor.ActorID.ASC(),
    Film.FilmID.ASC(),
)
```
_Package(dot) import is used, so the statements look as close as possible to the native SQL._  
Note that every column has a type. String column `Language.Name` and `Category.Name` can be compared only with 
string columns and expressions. `Actor.ActorID`, `FilmActor.ActorID`, `Film.Length` are integer columns 
and can be compared only with integer columns and expressions.

__How to get a parametrized SQL query from the statement?__
```go
query, args := stmt.Sql()
```
query - parametrized query  
args - query parameters 

<details>
  <summary>Click to see `query` and `args`</summary>
  
```sql
SELECT actor.actor_id AS "actor.actor_id",
       actor.first_name AS "actor.first_name",
       actor.last_name AS "actor.last_name",
       actor.last_update AS "actor.last_update",
       film.film_id AS "film.film_id",
       film.title AS "film.title",
       film.description AS "film.description",
       film.release_year AS "film.release_year",
       film.language_id AS "film.language_id",
       film.rental_duration AS "film.rental_duration",
       film.rental_rate AS "film.rental_rate",
       film.length AS "film.length",
       film.replacement_cost AS "film.replacement_cost",
       film.rating AS "film.rating",
       film.last_update AS "film.last_update",
       film.special_features AS "film.special_features",
       film.fulltext AS "film.fulltext",
       language.language_id AS "language.language_id",
       language.name AS "language.name",
       language.last_update AS "language.last_update",
       category.category_id AS "category.category_id",
       category.name AS "category.name",
       category.last_update AS "category.last_update"
FROM dvds.actor
         INNER JOIN dvds.film_actor ON (actor.actor_id = film_actor.actor_id)
         INNER JOIN dvds.film ON (film.film_id = film_actor.film_id)
         INNER JOIN dvds.language ON (language.language_id = film.language_id)
         INNER JOIN dvds.film_category ON (film_category.film_id = film.film_id)
         INNER JOIN dvds.category ON (category.category_id = film_category.category_id)
WHERE ((language.name = $1::char(20)) AND (category.name != $2::text)) AND (film.length > $3::integer)
ORDER BY actor.actor_id ASC, film.film_id ASC;
```
```sh 
[English Action 180]
```


</details>
    
__How to get debug SQL from statement?__  
 ```go
debugSql := stmt.DebugSql()
```
debugSql - this query string can be copy-pasted to sql editor and executed. __It is not intended to be used in production. For debug purposes only!!!__

<details>
  <summary>Click to see debug sql</summary>
  
```sql
SELECT actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update",
     film.film_id AS "film.film_id",
     film.title AS "film.title",
     film.description AS "film.description",
     film.release_year AS "film.release_year",
     film.language_id AS "film.language_id",
     film.rental_duration AS "film.rental_duration",
     film.rental_rate AS "film.rental_rate",
     film.length AS "film.length",
     film.replacement_cost AS "film.replacement_cost",
     film.rating AS "film.rating",
     film.last_update AS "film.last_update",
     film.special_features AS "film.special_features",
     film.fulltext AS "film.fulltext",
     language.language_id AS "language.language_id",
     language.name AS "language.name",
     language.last_update AS "language.last_update",
     category.category_id AS "category.category_id",
     category.name AS "category.name",
     category.last_update AS "category.last_update"
FROM dvds.actor
     INNER JOIN dvds.film_actor ON (actor.actor_id = film_actor.actor_id)
     INNER JOIN dvds.film ON (film.film_id = film_actor.film_id)
     INNER JOIN dvds.language ON (language.language_id = film.language_id)
     INNER JOIN dvds.film_category ON (film_category.film_id = film.film_id)
     INNER JOIN dvds.category ON (category.category_id = film_category.category_id)
WHERE ((language.name = 'English'::char(20)) AND (category.name != 'Action'::text)) AND (film.length > 180::integer)
ORDER BY actor.actor_id ASC, film.film_id ASC;
```
</details>


#### Execute query and store result

Well-formed SQL is just a first half of the job. Let's see how can we make some sense of result set returned executing 
above statement. Usually this is the most complex and tedious work, but with Jet it is the easiest.

First we have to create desired structure to store query result. 
This is done be combining autogenerated model types, or it can be done 
by combining custom model types(see [wiki](https://github.com/go-jet/jet/wiki/Query-Result-Mapping-(QRM)#custom-model-types) for more information).  

_Note that it's possible to overwrite default jet generator behavior. All the aspects of generated model and SQLBuilder types can be 
tailor-made([wiki](https://github.com/go-jet/jet/wiki/Generator#generator-customization))._

Let's say this is our desired structure made of autogenerated types:  
```go
var dest []struct {
    model.Actor
    
    Films []struct {
        model.Film
        
        Language    model.Language
        Categories  []model.Category
    }
}
```
`Films` field is a slice because one actor can act in multiple films, and because each film belongs to one language
`Langauge` field is just a single model struct. `Film` can belong to multiple categories.  
_*There is no limitation of how big or nested destination can be._

Now let's execute above statement on open database connection (or transaction) db and store result into `dest`.

```go
err := stmt.Query(db, &dest)
handleError(err)
```

__And that's it.__
  
`dest` now contains the list of all actors(with list of films acted, where each film has information about language and list of belonging categories) that acted in films longer than 180 minutes, film language is 'English' 
and film category is not 'Action'.

Lets print `dest` as a json to see:
```go
jsonText, _ := json.MarshalIndent(dest, "", "\t")
fmt.Println(string(jsonText))
```

```js
[
	{
		"ActorID": 1,
		"FirstName": "Penelope",
		"LastName": "Guiness",
		"LastUpdate": "2013-05-26T14:47:57.62Z",
		"Films": [
			{
				"FilmID": 499,
				"Title": "King Evolution",
				"Description": "A Action-Packed Tale of a Boy And a Lumberjack who must Chase a Madman in A Baloon",
				"ReleaseYear": 2006,
				"LanguageID": 1,
				"RentalDuration": 3,
				"RentalRate": 4.99,
				"Length": 184,
				"ReplacementCost": 24.99,
				"Rating": "NC-17",
				"LastUpdate": "2013-05-26T14:50:58.951Z",
				"SpecialFeatures": "{Trailers,\"Deleted Scenes\",\"Behind the Scenes\"}",
				"Fulltext": "'action':5 'action-pack':4 'baloon':21 'boy':10 'chase':16 'evolut':2 'king':1 'lumberjack':13 'madman':18 'must':15 'pack':6 'tale':7",
				"Language": {
					"LanguageID": 1,
					"Name": "English             ",
					"LastUpdate": "0001-01-01T00:00:00Z"
				},
				"Categories": [
					{
						"CategoryID": 8,
						"Name": "Family",
						"LastUpdate": "2006-02-15T09:46:27Z"
					}
				]
			}
		]
	},
	{
		"ActorID": 3,
		"FirstName": "Ed",
		"LastName": "Chase",
		"LastUpdate": "2013-05-26T14:47:57.62Z",
		"Films": [
			{
				"FilmID": 996,
				"Title": "Young Language",
				"Description": "A Unbelieveable Yarn of a Boat And a Database Administrator who must Meet a Boy in The First Manned Space Station",
				"ReleaseYear": 2006,
				"LanguageID": 1,
				"RentalDuration": 6,
				"RentalRate": 0.99,
				"Length": 183,
				"ReplacementCost": 9.99,
				"Rating": "G",
				"LastUpdate": "2013-05-26T14:50:58.951Z",
				"SpecialFeatures": "{Trailers,\"Behind the Scenes\"}",
				"Fulltext": "'administr':12 'boat':8 'boy':17 'databas':11 'first':20 'languag':2 'man':21 'meet':15 'must':14 'space':22 'station':23 'unbeliev':4 'yarn':5 'young':1",
				"Language": {
					"LanguageID": 1,
					"Name": "English             ",
					"LastUpdate": "0001-01-01T00:00:00Z"
				},
				"Categories": [
					{
						"CategoryID": 6,
						"Name": "Documentary",
						"LastUpdate": "2006-02-15T09:46:27Z"
					}
				]
			}
		]
	},
	//...(125 more items)
]
```

What if, we also want to have list of films per category and actors per category, where films are longer than 180 minutes, film language is 'English' 
and film category is not 'Action'.  
In that case we can reuse above statement `stmt`, and just change our destination:

```go
var dest2 []struct {
    model.Category

    Films []model.Film
    Actors []model.Actor
}

err = stmt.Query(db, &dest2)
handleError(err)
```
<details>
  <summary>Click to see `dest2` json</summary>

```js
[
	{
		"CategoryID": 8,
		"Name": "Family",
		"LastUpdate": "2006-02-15T09:46:27Z",
		"Films": [
			{
				"FilmID": 499,
				"Title": "King Evolution",
				"Description": "A Action-Packed Tale of a Boy And a Lumberjack who must Chase a Madman in A Baloon",
				"ReleaseYear": 2006,
				"LanguageID": 1,
				"RentalDuration": 3,
				"RentalRate": 4.99,
				"Length": 184,
				"ReplacementCost": 24.99,
				"Rating": "NC-17",
				"LastUpdate": "2013-05-26T14:50:58.951Z",
				"SpecialFeatures": "{Trailers,\"Deleted Scenes\",\"Behind the Scenes\"}",
				"Fulltext": "'action':5 'action-pack':4 'baloon':21 'boy':10 'chase':16 'evolut':2 'king':1 'lumberjack':13 'madman':18 'must':15 'pack':6 'tale':7"
			},
			{
				"FilmID": 50,
				"Title": "Baked Cleopatra",
				"Description": "A Stunning Drama of a Forensic Psychologist And a Husband who must Overcome a Waitress in A Monastery",
				"ReleaseYear": 2006,
				"LanguageID": 1,
				"RentalDuration": 3,
				"RentalRate": 2.99,
				"Length": 182,
				"ReplacementCost": 20.99,
				"Rating": "G",
				"LastUpdate": "2013-05-26T14:50:58.951Z",
				"SpecialFeatures": "{Commentaries,\"Behind the Scenes\"}",
				"Fulltext": "'bake':1 'cleopatra':2 'drama':5 'forens':8 'husband':12 'monasteri':20 'must':14 'overcom':15 'psychologist':9 'stun':4 'waitress':17"
			}
		],
		"Actors": [
			{
				"ActorID": 1,
				"FirstName": "Penelope",
				"LastName": "Guiness",
				"LastUpdate": "2013-05-26T14:47:57.62Z"
			},
			{
				"ActorID": 20,
				"FirstName": "Lucille",
				"LastName": "Tracy",
				"LastUpdate": "2013-05-26T14:47:57.62Z"
			},
			{
				"ActorID": 36,
				"FirstName": "Burt",
				"LastName": "Dukakis",
				"LastUpdate": "2013-05-26T14:47:57.62Z"
			},
			{
				"ActorID": 70,
				"FirstName": "Michelle",
				"LastName": "Mcconaughey",
				"LastUpdate": "2013-05-26T14:47:57.62Z"
			},
			{
				"ActorID": 118,
				"FirstName": "Cuba",
				"LastName": "Allen",
				"LastUpdate": "2013-05-26T14:47:57.62Z"
			},
			{
				"ActorID": 187,
				"FirstName": "Renee",
				"LastName": "Ball",
				"LastUpdate": "2013-05-26T14:47:57.62Z"
			},
			{
				"ActorID": 198,
				"FirstName": "Mary",
				"LastName": "Keitel",
				"LastUpdate": "2013-05-26T14:47:57.62Z"
			}
		]
	},
    //...
]
```
</details>

Complete code example can be found at [./examples/quick-start/quick-start.go](./examples/quick-start/quick-start.go)


This example represent probably the most common use case.  Detail info about additional statements, features and use cases can be 
found at project [Wiki](https://github.com/go-jet/jet/wiki) page.

## Benefits

What are the benefits of writing SQL in Go using Jet?  
The biggest benefit is speed. Speed is being improved in 3 major areas:

##### Speed of development  

Writing SQL queries is faster and easier, as developers will have help of SQL code completion and SQL type safety directly from Go code.
Automatic scan to arbitrary structure removes a lot of headache and boilerplate code needed to structure database query result.  

##### Speed of execution

While ORM libraries can introduce significant performance penalties due to number of round-trips to the database(N+1 query problem), 
`jet` will always perform better as developers can write complex query and retrieve result with a single database call. 
Thus handler time lost on latency between server and database can be constant. Handler execution will be proportional 
only to the query complexity and the number of rows returned from database. 

With Jet, it is even possible to join the whole database and store the whole structured result in one database call. 
This is exactly what is being done in one of the tests: [TestJoinEverything](https://github.com/go-jet/jet/blob/6706f4b228f51cf810129f57ba90bbdb60b85fe7/tests/postgres/chinook_db_test.go#L187). 
The whole test database is joined and query result(~10,000 rows) is stored in a structured variable in less than 0.5s. 

##### How quickly bugs are found

The most expensive bugs are the one discovered on the production, and the least expensive are those found during development.
With automatically generated type safe SQL, not only queries are written faster but bugs are found sooner.  
Let's return to quick start example, and take closer look at a line:
 ```go
AND(Film.Length.GT(Int32(180))),
```
Let's say someone changes column `length` to `duration` from `film` table. The next go build will fail at that line, and 
the bug will be caught at compile time.

Let's say someone changes the type of `length` column to some non-integer type. Build will also fail at the same line
because integer columns and expressions can be only compared to other integer columns and expressions.

Build will also fail if someone removes `length` column from `film` table. `Film` field will be omitted from SQL Builder and Model types, 
next time `jet` generator is run.

Without Jet these bugs will have to be either caught by tests or by manual testing. 

## Dependencies
At the moment Jet dependence only of:
- `github.com/lib/pq` _(Used by jet generator to read `PostgreSQL` database information)_
- `github.com/go-sql-driver/mysql` _(Used by jet generator to read `MySQL` and `MariaDB` database information)_
- `github.com/mattn/go-sqlite3` _(Used by jet generator to read `SQLite` database information)_ 
- `github.com/google/uuid` _(Used in data model files and for debug purposes)_
  
To run the tests, additional dependencies are required:
- `github.com/pkg/profile`
- `github.com/stretchr/testify`
- `github.com/google/go-cmp`
- `github.com/jackc/pgx/v4`
- `github.com/shopspring/decimal`
- `github.com/volatiletech/null/v8`

## Versioning

[SemVer](http://semver.org/) is used for versioning. For the versions available, take a look at the [releases](https://github.com/go-jet/jet/releases).  

## License

Copyright 2019-2024 Goran Bjelanovic  
Licensed under the Apache License, Version 2.0.
