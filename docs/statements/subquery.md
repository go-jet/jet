# Sub-query

## How to write sub-queries

Sub-queries are composed first:

```go
// select film_id, title and rating of films with 'R' rating
rRatingFilms :=
	SELECT(
		Film.FilmID,
		Film.Title,
		Film.Rating,
	).FROM(
		Film,
	).WHERE(
		Film.Rating.EQ(enum.MpaaRating.R),
	).AsTable("rFilms")
```

`AsTable("rFilms")` allows a SELECT statement to be used as a table source in a `FROM` clause.

To use sub-query columns in the main statement, export them from the sub-query with the `From` method:

```go
rFilmID := Film.FilmID.From(rRatingFilms) // <- used for the join condition
```

Now the main query can be written:

```go
query := SELECT(
		rRatingFilms.AllColumns(),
		Actor.AllColumns,
	).FROM(
		rRatingFilms.
			INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(rFilmID)).
			INNER_JOIN(Actor, FilmActor.ActorID.EQ(Actor.ActorID)),
	)
```

`rRatingFilms.AllColumns()` exports all sub-query columns for projection. The effect is the same as exporting
each column one by one with `From`.

Debug SQL of the example:

```sql
SELECT "rFilms"."film.film_id" AS "film.film_id",           -- <- the same alias names as in the sub-query
     "rFilms"."film.title" AS "film.title",
     "rFilms"."film.rating" AS "film.rating",
     actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update"
FROM (
          SELECT film.film_id AS "film.film_id",              -- <- these aliases reappear in the main query
               film.title AS "film.title",
               film.rating AS "film.rating"
          FROM dvds.film
          WHERE film.rating = 'R'
     ) AS "rFilms"
     INNER JOIN dvds.film_actor ON (film_actor.film_id = "rFilms"."film.film_id")
     INNER JOIN dvds.actor ON (film_actor.actor_id = actor.actor_id);
```

Column aliases in the main query are the same as in the sub-query, because default column aliases are
passed through from the sub-query to the surrounding query. They are NOT affected by the sub-query alias
(`rFilms`).

Since the aliases match the default table alias, the generated model types can be used as the scan destination
without modification:

```go
var dest []struct {
	model.Film // <- data from the sub-query

	Actors []model.Actor
}

err := query.Query(db, &dest)
```

The same logic applies to common table expressions used in [WITH statements](./with).

### Custom named destination type

Suppose the destination is now a `MyFilm` struct:

```go
type MyFilm struct {
	Name     string        // <- !! there is no matching projection alias
	Duration time.Duration // <- !!

	Actors []model.Actor
}

var dest []MyFilm
```

Scan would not work for `Name` and `Duration`, because there is no projection aliased `"my_film.name"` or
`"my_film.duration"`. To fix it, either add an [alias tag on the destination](../guide/qrm#tagging-model-type-fields)
or update the alias in the main query:

```go
query := SELECT(
		rRatingFilms.AllColumns().AS("my_film.*"), // changes the alias of all sub-query projections
		Actor.AllColumns,
	)...
```

The debug SQL now contains correctly aliased projections:

```sql
SELECT "rFilms"."film.film_id" AS "my_film.film_id",       -- renamed from film.film_id to my_film.film_id
     "rFilms"."film.title" AS "my_film.title",
     "rFilms"."film.rating" AS "my_film.rating",
     ...
```

### Selecting a subset of sub-query projections

The list of sub-query projections can be reduced with `Except`:

```go
query := SELECT(
		rRatingFilms.AllColumns().Except(Film.Title, Film.Rating),
		Actor.AllColumns,
	)...
```

`film.title` and `film.rating` no longer appear in the generated SQL:

```sql
SELECT "rFilms"."film.film_id" AS "film.film_id",
     actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     ...
```

### Sub-query projection that is not a table column

For instance:

```go
customersPayments := SELECT(
        Payment.CustomerID,
        SUMf(Payment.Amount).AS("amount_sum"),
    ).FROM(
        Payment,
    ).GROUP_BY(
        Payment.CustomerID,
    ).AsTable("customer_payment_sum")

customerID := Payment.CustomerID.From(customersPayments)
```

To export `"amount_sum"` from the sub-query, first create a column with the appropriate type and name. Since
`SUMf` returns a float expression, create a `FloatColumn` named `"amount_sum"` and export it:

```go
amountSum := FloatColumn("amount_sum").From(customersPayments)
```

This is only required if `amount_sum` needs to appear in main query conditions (for example, in a JOIN or
WHERE). Otherwise `amount_sum` can be exported the usual way, with `AllColumns()`.

## Lateral queries

Lateral queries have similar syntax to sub-queries and can be used in a `FROM` clause like tables or
sub-queries. Unlike a plain sub-query, a lateral sub-query can reference columns of preceding `FROM` items.

```go
languages := LATERAL(
	SELECT(
		Language.AllColumns,
	).FROM(
		Language,
	).WHERE(
		Language.Name.NOT_IN(String("spanish")).
			AND(Film.LanguageID.EQ(Language.LanguageID)), // Film.LanguageID is from the main statement
	),
).AS("languages")

stmt := SELECT(
        Film.FilmID,
        Film.Title,
        languages.AllColumns(),
    ).FROM(
        Film,
        languages,
    ).WHERE(
        Film.FilmID.EQ(Int(1)),
    )
```
