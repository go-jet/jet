# Jet

[![CircleCI](https://circleci.com/gh/go-jet/jet/tree/develop.svg?style=svg&circle-token=97f255c6a4a3ab6590ea2e9195eb3ebf9f97b4a7)](https://circleci.com/gh/go-jet/jet/tree/develop)

Jet is Go SQL Builder for PostgreSQL(support for MySql and OracleSql will be added later). 
Jet enables writing type safe SQL queries in Go, and has ability to convert database query result to desired arbitrary structure.

## Contents
   - [Getting Started](#getting-started)
      - [Prerequisites](#prerequisites)
      - [Installation](#installation)
      - [Quick Start](#quick-start)
         - [Generate sql builder and model files](#generate-sql-builder-and-model-files)
         - [Lets write some SQL queries in Go](#lets-write-some-sql-queries-in-go)
   - [Benefits](#benefits)
   - [Versioning](#versioning)

## Features
* TODO

## Getting Started

### Prerequisites

To install Jet package, you need to install Go and set your Go workspace first.

[Go](https://golang.org/) **version 1.8+ is required**

### Installation

Use the bellow command to install jet
```sh
$ go get -u github.com/go-jet/jet
```

Install jetgen to GOPATH bin folder. This will allow generating jet files from the command line.

```sh
go install github.com/go-jet/jet/cmd/jetgen
```

Make sure GOPATH bin folder is added to the PATH environment variable.

### Quick Start
For this quick start example we will use sample _dvd rental_ database. Full database dump can be found in [./tests/init/data/dvds.sql](./tests/init/data/dvds.sql).
Schema diagram of interest for example can be found [here](./examples/quick-start/diagram.png).

#### Generate sql builder and model files
To generate Go sql builder and Go data model from postgres database we need to call jetgen, and provide it with postgres connection parameters and destination folder for generated go files.\
Assuming we are running local postgres database, with user `jet`, database `jetdb` and schema `dvds` we will use this command:
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
As jetgen command output suggest, jetgen will:  
- connect to postgres database and retrieve information about the tables and enums of `dvds` schema
- delete everything in destination folder `./gen`,   
- and finally generate sql builder and model Go files for each schema tables and enums into destination folder `./gen`.  


Generated files folder structure will look like this:
```sh 
|-- gen                               # destination folder
|   `-- jetdb                         # database name
|       `-- dvds                      # schema name
|           |-- enum                  # sql builder folder for enums
|           |   |-- mpaa_rating.go
|           |-- table                 # sql builder folder for tables
|               |-- actor.go
|               |-- address.go
|               |-- category.go
                ...
|           |-- model                 # Plain Old Data for every enum and table
|           |   |-- actor.go
|           |   |-- address.go
|           |   |-- category.go
                ...

```
You will be using types from `table` and `enum` to write type safe SQL in Go, and `model` types would be used to store results of the queries.
#### Lets write some SQL queries in Go

First lets import jet and generated files from previous step
```go
import (
	. "github.com/go-jet/jet"                                           // dot import so go code would resemble as much as native SQL
	. "github.com/go-jet/jet/examples/quick-start/gen/jetdb/dvds/table" // dot import is not mandatory

	"github.com/go-jet/jet/examples/quick-start/gen/jetdb/dvds/model"
)
```
Lets say we want to retrieve the list of all actors that acted in films longer than 180 minutes, film language is 'English' 
and film category is not 'Action'.  
```go
stmt := SELECT(
    Actor.ActorID, Actor.FirstName, Actor.LastName, Actor.LastUpdate, // list of all actor columns (equivalent to Actor.AllColumns)
    Film.AllColumns,                                                  // list of all film columns (equivalent to Film.FilmID, Film.Title, ...)
    Language.AllColumns,
    Category.AllColumns,
).FROM(
    Actor.
        INNER_JOIN(FilmActor, Actor.ActorID.EQ(FilmActor.ActorID)).  // INNER JOIN Actor with FilmActor on condition Actor.ActorID = FilmActor.ActorID
        INNER_JOIN(Film, Film.FilmID.EQ(FilmActor.FilmID)).          // then with Film, Language, FilmCategory and Category.
        INNER_JOIN(Language, Language.LanguageID.EQ(Film.LanguageID)).
        INNER_JOIN(FilmCategory, FilmCategory.FilmID.EQ(Film.FilmID)).
        INNER_JOIN(Category, Category.CategoryID.EQ(FilmCategory.CategoryID)),
).WHERE(
    Language.Name.EQ(String("English")).                    // every column has type.
        AND(Category.Name.NOT_EQ(String("Action"))).        // String column Language.Name and Category.Name can be compared only with string columns and expressions
        AND(Film.Length.GT(Int(180))),                      // Film.Length is integer column and can be compared only with integer columns and expressions
).ORDER_BY(
    Actor.ActorID.ASC(),
    Film.FilmID.ASC(),
)
```
To see sql formed with this statement:
```go
query, args, err := stmt.Sql()
```
query - is parametrized query\
args - are parameters for the query

<details>
  <summary>Click to see</summary>
  
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
WHERE ((language.name = $1) AND (category.name != $2)) AND (film.length > $3)
ORDER BY actor.actor_id ASC, film.film_id ASC;
```
```sh 
[English Action 180]
```


</details>
  
To see debug sql that can be copy pasted to sql editor and executed.
```go
query, err := stmt.DebugSql()
```
query - is parametrized query where every parameter is replaced with appropriate string argument representation
<details>
  <summary>Click to see</summary>
  
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
WHERE ((language.name = 'English') AND (category.name != 'Action')) AND (film.length > 180)
ORDER BY actor.actor_id ASC, film.film_id ASC;
```
</details>

Well formed sql is just a first half the job. Now lets execute sql statement and store result in desired structure.  
Let's say this is our desired structure:  
```go
var dest []struct {
    model.Actor
    Films []struct {
        model.Film
        Language model.Language
        Categories []model.Category
    }
}
```
_There is no limitation for how big or nested destination structure can be._

Now to lets execute a above statement on open database connection db and store result into `dest`.

```go
err := stmt.Query(db, &dest)
handleError(err)
```

And thats it. `dest` now contains the list of all actors(with list of films acted, where each film has information about language and list of belonging categories) that acted in films longer than 180 minutes, film language is 'English' 
and film category is not 'Action'.

Lets print `dest` as a json to see:
```go
jsonText, _ := json.MarshalIndent(dest, "", "\t")
fmt.Println(string(jsonText))
```

```json
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
					"LastUpdate": "2006-02-15T10:02:19Z"
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
					"LastUpdate": "2006-02-15T10:02:19Z"
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
	...(125 more items)
```

What if we also want to have list of films per category and actors per category, where films are longer than 180 minutes, film language is 'English' 
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
  <summary>Click to see dest2 json</summary>

```json
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
    ...
```
</details>

Complete code example can be found at [./examples/quick-start/quick-start.go](./examples/quick-start/quick-start.go)


This example represent probably the most common use case, but Jet offers much more. Like subqueries, INSERT, UPDATE, DELETE, LOCK statements and much more.
Detail info can be found at project wiki page.

## Benefits

##### What are the benefits of writing SQL in Go using Jet?
The biggest benefit is speed. Speed is improved in 3 major areas:
##### Speed of development  
Writing SQL queries is much easier directly from Go, because programmer will have the help of SQL code completion and SQL type safety directly in Go.
Writing code is much faster and code is more robust. Automatic scan to arbitrary structure removes a lot of headache and 
boilerplate code needed to structure database query result.  
With Jet programmer have the power of SQL but also ease of use of NoSQL. 
##### Speed of execution
Common web and database server usually is not on the same physical machine, and there is some latency between them. 
Latency can vary from 5ms to 50+ms. In majority of cases query executed on database is simple query lasting no more than 1ms.
In those cases web server handler execution time is directly proportional to latency between server and database.
This is not such a big problem if handler calls database couple of times, but what if web server is using ORM to retrieve all data from database.
ORM usually access the database once for every object needed.  
Now lets say latency is 30ms and there are 100 different objects required from the database. This handler will last 3s !!!.  
With Jet, handler time lost on latency between server and database is constant. Because we can write complex query and 
return result in one database call. Handler execution will be proportional to number or rows returned from database. 
ORM example replaced with jet will take just 30ms + 'result scan time' = 31ms (rough estimate).  

With Jet you can even join the whole database and store the whole structured result in  in one query call. 
This is exactly what is being done in one of the tests: [TestJoinEverything](/tests/chinook_db_test.go#L40). 
The whole test database is joined and query result is stored in a structured variable in less than 1s. 

##### How quickly bugs are found
The most expensive bugs are the one on the production and the least expensive are those found during development.
With automatically generated type safe SQL not only queries are written faster but bugs are found sooner.  
Lets return to quick start example, and take closer look at a line:
 ```go
AND(Film.Length.GT(Int(180))), // Film.Length is integer column and can be compared only with integer columns and expressions
```
Lets say someone changes column `length` to `duration` from `film` table. The next go build will fail at that line and 
the bug will be caught at compile time.

Lets say someone changes the type of `length` column to some non integer type. Build will also fail at the same line
because integer columns and expressions can be only compered to other integer columns and expressions.

Without Jet these bugs will have to be either caught by some test or by manual testing. 

## Contributing

## Versioning

[SemVer](http://semver.org/) is used for versioning. For the versions available, see the [releases](https://github.com/go-jet/jet/releases). 

## Licence