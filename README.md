# Jet

<img align="right" width="175px" src="https://github.com/go-jet/jet/wiki/image/mascot.png">

[![go-jet](https://circleci.com/gh/go-jet/jet.svg?style=svg)](https://app.circleci.com/pipelines/github/go-jet/jet?branch=master)
[![codecov](https://codecov.io/gh/go-jet/jet/branch/master/graph/badge.svg)](https://codecov.io/gh/go-jet/jet)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-jet/jet)](https://goreportcard.com/report/github.com/go-jet/jet/v2)
[![Documentation](https://godoc.org/github.com/go-jet/jet?status.svg)](http://godoc.org/github.com/go-jet/jet/v2)
[![GitHub release](https://img.shields.io/github/release/go-jet/jet.svg)](https://github.com/go-jet/jet/releases)

Jet is a complete solution for efficient and high performance database access, combining a type-safe SQL builder with code generation
and automatic query result mapping.

Jet currently supports the following database engines:

* `PostgreSQL`
* `MySQL`
* `SQLite`

This list is not exclusive, as many other databases implement compatible wire protocols. For example, `CockroachDB` uses the
`PostgreSQL` wire protocol, and `MariaDB` is based on the `MySQL` protocol. Both databases are tested and known to work with Jet.

Support for additional databases may be introduced in future releases.

![jet](https://github.com/go-jet/jet/wiki/image/jet.png)  
Jet is the easiest, and the fastest way to write complex type-safe SQL queries as a Go code and map database query result
into complex object composition.

> [!Note]
> Jet is __not__ an ORM.

## Contents
- [Motivation](#motivation)
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
- [Support the Project](#support-the-project)

## Motivation
https://medium.com/@go.jet/jet-5f3667efa0cc

## Features
1) Auto-generated type-safe SQL Builder. Statements supported:
    * [SELECT](https://github.com/go-jet/jet/wiki/SELECT), [SELECT_JSON](https://github.com/go-jet/jet/wiki/SELECT_JSON-Statements) `(DISTINCT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, FOR, LOCK_IN_SHARE_MODE, UNION, INTERSECT, EXCEPT, WINDOW, sub-queries)`
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

[Go](https://golang.org/) **version 1.22+ is required**

### Installation

Use the command bellow to add jet as a dependency into `go.mod` project:
```sh
$ go get -u github.com/go-jet/jet/v2
```

Jet generator can be installed using one of the following methods:

- ✅ Option 1: Install via go install:
```sh
go install github.com/go-jet/jet/v2/cmd/jet@latest
```

> [!Tip]
> Jet generator is installed to the directory named by the `GOBIN` environment variable,
which defaults to `$GOPATH/bin` or `$HOME/go/bin` if the `GOPATH` environment variable is not set.

- ✅ Option 2: Build manually from source and install jet generator to specific folder:
```sh
git clone https://github.com/go-jet/jet.git
cd jet && go build -o <target_directory> ./cmd/jet
```
> [!Tip]
> Make sure `target_directory` is included in your system’s `PATH` environment variable to allow global access to the jet command.


### Quick Start
For this quick start example we will use PostgreSQL sample _'dvd rental'_ database. Full database dump can be found in
[./tests/testdata/init/postgres/dvds.sql](https://github.com/go-jet/jet-test-data/blob/master/init/postgres/dvds.sql).
A schema diagram illustrating the relevant part of the database is available [here](./examples/quick-start/diagram.png).

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

As indicated by the command output, Jet will perform the following actions:
- ✅ Connect to the PostgreSQL database and retrieve metadata for all `tables`, `views`, and `enums` within the `dvds` schema.
- ⚠️ **Delete all contents** in the target schema folder: `./.gen/jetdb/dvds`.
- ⚙️ Generate **SQL Builder** and **Data Model** types for each table, view, and enum found in the schema.

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

Types from the `table`, `view`, and `enum` packages are used to write **type-safe SQL queries in Go**, while types from the `model` types are combined to store
results of the SQL queries.

> [!Note]
> It is possible to customize the default Jet generator behavior. All the aspects of generated SQLBuilder and model types 
> are customizable(see [wiki](https://github.com/go-jet/jet/wiki/Generator#generator-customization)).


#### Let's write some SQL queries in Go

First we need to import postgres SQLBuilder and generated packages from the previous step:
```go
import (
  // dot import so go code would resemble as much as native SQL
  // dot import is not mandatory
  . "github.com/go-jet/jet/v2/examples/quick-start/.gen/jetdb/dvds/table"
  . "github.com/go-jet/jet/v2/postgres"

  "github.com/go-jet/jet/v2/examples/quick-start/.gen/jetdb/dvds/enum"
  "github.com/go-jet/jet/v2/examples/quick-start/.gen/jetdb/dvds/model"  
)
```
Let's say we want to retrieve the list of all _actors_ who acted in _films_ longer than 180 minutes, _film language_ is 'English', 
_film category_ is not 'Action' and _film rating_ is not 'R'.
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
        AND(Film.Length.GT(Int32(180))).
        AND(Film.Rating.NOT_EQ(enum.MpaaRating.R)),              
).ORDER_BY(
    Actor.ActorID.ASC(),
    Film.FilmID.ASC(),
)
```
> [!Tip]
> Package(dot) import is used, so the statements look as close as possible to the native SQL.  

Note that every column has a type. String columns, such as `Language.Name` and `Category.Name` can only be compared with
string columns and expressions. Similarity, `Actor.ActorID`, `FilmActor.ActorID`, `Film.Length` are integer columns
and can only be compared with integer columns and expressions.

__How to Get a Parametrized SQL Query from the Statement?__
```go
query, args := stmt.Sql()
```
query - parametrized query  
args - query arguments

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
WHERE (((language.name = $1::char(20)) AND (category.name != $2::text)) AND (film.length > $3)) AND (film.rating != 'R')
ORDER BY actor.actor_id ASC, film.film_id ASC;
```
```sh 
[English Action 180]
```


</details>

__How to Get Debug SQL from Statement?__
 ```go
debugSql := stmt.DebugSql()
```
debugSql - this query string can be copy-pasted into sql editor and executed. 
> [!Warning]
> Debug SQL is not intended to be used in production. For debug purposes only!!!

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
WHERE (((language.name = 'English'::char(20)) AND (category.name != 'Action'::text)) AND (film.length > 180)) AND (film.rating != 'R')
ORDER BY actor.actor_id ASC, film.film_id ASC;
```
</details>


#### Execute Query and Store Result

Well-formed SQL is just a first half of the job. Let's see how can we make some sense of result set returned executing
above statement. Usually this is the most complex and tedious work, but with Jet it is the easiest.

First, we need to define the structure in which to store the query result. This
can be achieved by combining autogenerated model types, or by using custom
model types(see [wiki](https://github.com/go-jet/jet/wiki/Query-Result-Mapping-(QRM)#custom-model-types) for more information).

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

The `Films` field is a slice because an actor can appear in multiple films,
and each film is associated with a single language. The `Language` field,
on the other hand, is a single model struct. A `Film` can belong to multiple
categories.
 
> [!Note]
> There is no limitation of how big or nested destination can be.

Now, let's execute the above statement on an open database connection (or
transaction) `db` and store the result in the `dest` variable.

```go
err := stmt.Query(db, &dest)
handleError(err)
```

__And that's it.__

The `dest` variable now contains a list of all actors (each with a list of
films they acted in). Each film includes information about its language and
a list of categories it belongs to. This list is filtered to include only
films longer than 180 minutes, where the film language is 'English', and
the film category is not 'Action'.

> [!Tip]  
> It is recommended to enable **Strict Scan** on application startup, especially when destination contains 
> custom model types. For more details, see the [wiki](https://github.com/go-jet/jet/wiki/Query-Result-Mapping-(QRM)#strict-scan).

Lets print `dest` as a JSON to see:
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

##### Speed of Development

Writing SQL queries becomes faster and more efficient, as developers benefit
from SQL code completion and type safety directly within Go code. Automatic
scanning to arbitrary structures eliminates much of the headache and boilerplate required
to structure database query results, reducing both complexity and development time.

##### Speed of Execution

While ORM libraries can introduce significant performance penalties when
multiple tables are involved, due to multiple round-trips to the database
(i.e., the `N+1` query problem), Jet will always perform better. Developers
can write queries of any complexity and retrieve results with a single database call.
As a result, handler time lost to latency between the server and database
remains constant. Handler execution time is proportional only to query
complexity and the number of rows returned from the database.

> [!Tip]
> As of the version `v2.13.0`, developers can leverage [SELECT_JSON](https://github.com/go-jet/jet/wiki/SELECT_JSON-Statements) 
> statements to encode query result as JSON on the SQL server, and then return that JSON as a result. Instead of 
> returning a large set of rows, the query now returns a single row with one column containing the entire result as JSON, 
> further reducing latency.

With Jet, it is even possible to join the whole database and store the whole structured result in one database call.
This is exactly what is being done in one of the tests: [TestJoinEverything](https://github.com/go-jet/jet/blob/6706f4b228f51cf810129f57ba90bbdb60b85fe7/tests/postgres/chinook_db_test.go#L187).
The whole test database is joined and query result(~10,000 rows and 68 columns) is stored in a structured variable in less than 0.5s, or 
in case of `SELECT_JSON` statement in around 0.3s.

##### How Quickly Bugs are Found

The most expensive bugs are those discovered in production, while the least expensive are those found during development.
With automatically generated, type-safe SQL, queries are not only written faster but bugs are caught sooner.

Consider the following line from the quick start example:
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

To execute Jet SQL statements, **ANY** SQL driver that implements Go's standard `database/sql` interface can be used. 

By default, the `Jet` generator executable uses the following SQL drivers to read database schema information:
- `github.com/lib/pq` _(`PostgreSQL` and `CockroachDB` )_
- `github.com/go-sql-driver/mysql` _(`MySQL` and `MariaDB`)_
- `github.com/mattn/go-sqlite3` _(`SQLite`)_

The default SQL driver used by the Jet generator can be replaced, if needed, by 
[customizing the generator](https://github.com/go-jet/jet/wiki/Generator#generator-customization).

## Versioning

[SemVer](http://semver.org/) is used for versioning. For the versions available, take a look at the [releases](https://github.com/go-jet/jet/releases).
   
Typically, two releases are published each year — one in early spring and another in late autumn.

## License

Copyright 2019-2025 Goran Bjelanovic  
Licensed under the Apache License, Version 2.0.

## Support the Project

Ways to donate:
- [![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/E1E71CXDAE)
- BTC: bc1qtjhxe8mqx0yzff2l0f6stjpjj92kgwr0a53wxv
- ETH: 0xe98e4535C744c617e8E45828B63fDFf9367E3574