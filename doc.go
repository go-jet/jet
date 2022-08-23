/*
Package jet is a complete solution for efficient and high performance database access, consisting of type-safe SQL builder
with code generation and automatic query result data mapping.
Jet currently supports PostgreSQL, MySQL, MariaDB and SQLite. Future releases will add support for additional databases.

# Installation

Use the command bellow to add jet as a dependency into go.mod project:

	$ go get -u github.com/go-jet/jet/v2

Jet generator can be installed in one of the following ways:

 1. (Go1.16+) Install jet generator using go install:
    go install github.com/go-jet/jet/v2/cmd/jet@latest

 2. Install jet generator to GOPATH/bin folder:
    cd $GOPATH/src/ && GO111MODULE=off go get -u github.com/go-jet/jet/cmd/jet

 3. Install jet generator into specific folder:
    git clone https://github.com/go-jet/jet.git
    cd jet && go build -o dir_path ./cmd/jet

Make sure that the destination folder is added to the PATH environment variable.

# Usage

Jet requires already defined database schema(with tables, enums etc), so that jet generator can generate SQL Builder
and Model files. File generation is very fast, and can be added as every pre-build step.
Sample command:

	jet -dsn=postgresql://user:pass@localhost:5432/jetdb -schema=dvds -path=./.gen

Before we can write SQL queries in Go, we need to import generated SQL builder and model types:

	import . "some_path/.gen/jetdb/dvds/table"
	import "some_path/.gen/jetdb/dvds/model"

To write postgres SQL queries we import:

	. "github.com/go-jet/jet/v2/postgres" // Dot import is used so that Go code resemble as much as native SQL. It is not mandatory.

Then we can write the SQL query:

	// sub-query
	rRatingFilms :=
		SELECT(
			Film.FilmID,
			Film.Title,
			Film.Rating,
		).FROM(
			Film,
		).WHERE(
			Film.Rating.EQ(enum.FilmRating.R),
		).AsTable("rFilms")

	// export column from sub-query
	rFilmID := Film.FilmID.From(rRatingFilms)

	// main-query
	stmt :=
		SELECT(
			Actor.AllColumns,
			FilmActor.AllColumns,
			rRatingFilms.AllColumns(),
		).FROM(
			rRatingFilms.
				INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(rFilmID)).
				INNER_JOIN(Actor, Actor.ActorID.EQ(FilmActor.ActorID)
		).ORDER_BY(
			rFilmID,
			Actor.ActorID,
		)

Now we can run the statement and store the result into desired destination:

	var dest []struct {
		model.Film

		Actors []model.Actor
	}

	err := stmt.Query(db, &dest)

We can print a statement to see SQL query and arguments sent to postgres server:

	fmt.Println(stmt.Sql())

Output:

	SELECT "rFilms"."film.film_id" AS "film.film_id",
		 "rFilms"."film.title" AS "film.title",
		 "rFilms"."film.rating" AS "film.rating",
		 actor.actor_id AS "actor.actor_id",
		 actor.first_name AS "actor.first_name",
		 actor.last_name AS "actor.last_name",
		 actor.last_update AS "actor.last_update",
		 film_actor.actor_id AS "film_actor.actor_id",
		 film_actor.film_id AS "film_actor.film_id",
		 film_actor.last_update AS "film_actor.last_update"
	FROM (
			  SELECT film.film_id AS "film.film_id",
				   film.title AS "film.title",
				   film.rating AS "film.rating"
			  FROM dvds.film
			  WHERE film.rating = 'R'
		 ) AS "rFilms"
		 INNER JOIN dvds.film_actor ON (film_actor.film_id = "rFilms"."film.film_id")
		 INNER JOIN dvds.actor ON (film_actor.actor_id = actor.actor_id)
	WHERE "rFilms"."film.film_id" < $1
	ORDER BY "rFilms"."film.film_id" ASC, actor.actor_id ASC;
	 [50]

If we print destination as json, we'll get:

	[
		{
			"FilmID": 8,
			"Title": "Airport Pollock",
			"Rating": "R",
			"Actors": [
				{
					"ActorID": 55,
					"FirstName": "Fay",
					"LastName": "Kilmer",
					"LastUpdate": "2013-05-26T14:47:57.62Z"
				},
				{
					"ActorID": 96,
					"FirstName": "Gene",
					"LastName": "Willis",
					"LastUpdate": "2013-05-26T14:47:57.62Z"
				},
				...
			]
		},
		{
			"FilmID": 17,
			"Title": "Alone Trip",
			"Actors": [
				{
					"ActorID": 3,
					"FirstName": "Ed",
					"LastName": "Chase",
					"LastUpdate": "2013-05-26T14:47:57.62Z"
				},
				{
					"ActorID": 12,
					"FirstName": "Karl",
					"LastName": "Berry",
					"LastUpdate": "2013-05-26T14:47:57.62Z"
				},
				...
		...
	]

Detail info about all statements, features and use cases can be
found at project wiki page - https://github.com/go-jet/jet/wiki.
*/
package jet
