package mysql

import (
	"context"
	"fmt"
	"github.com/go-jet/jet/v2/qrm"
	"strings"
	"testing"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/table"

	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestSelectJsonObj(t *testing.T) {
	stmt := SELECT_JSON_OBJ(Actor.AllColumns).
		FROM(Actor).
		WHERE(Actor.ActorID.EQ(Int(2)))

	testutils.AssertStatementSql(t, stmt, `
SELECT JSON_OBJECT('actorID', actor.actor_id,
     'firstName', actor.first_name,
     'lastName', actor.last_name,
     'lastUpdate', actor.last_update) AS "json"
FROM dvds.actor
WHERE actor.actor_id = ?;
`, int64(2))

	var dest model.Actor

	err := stmt.QueryJSON(ctx, db, &dest)
	require.Nil(t, err)

	testutils.AssertDeepEqual(t, dest, actor2)
	requireLogged(t, stmt)
	requireQueryLogged(t, stmt, 1)
}

func TestSelectJsonObj_NestedObj(t *testing.T) {
	stmt := SELECT_JSON_OBJ(
		Actor.AllColumns,

		SELECT_JSON_OBJ(Film.AllColumns).
			FROM(FilmActor.INNER_JOIN(Film, Film.FilmID.EQ(FilmActor.FilmID))).
			WHERE(Actor.ActorID.EQ(FilmActor.ActorID)).
			ORDER_BY(Film.Length.DESC()).
			LIMIT(1).AS("LongestFilm"),
	).FROM(
		Actor,
	).WHERE(
		Actor.ActorID.EQ(Int(2)),
	)

	testutils.AssertStatementSql(t, stmt, `
SELECT JSON_OBJECT('actorID', actor.actor_id,
     'firstName', actor.first_name,
     'lastName', actor.last_name,
     'lastUpdate', actor.last_update,
     'LongestFilm', (
          SELECT JSON_OBJECT('filmID', film.film_id,
               'title', film.title,
               'description', film.description,
               'releaseYear', film.release_year,
               'languageID', film.language_id,
               'originalLanguageID', film.original_language_id,
               'rentalDuration', film.rental_duration,
               'rentalRate', film.rental_rate,
               'length', film.length,
               'replacementCost', film.replacement_cost,
               'rating', film.rating,
               'specialFeatures', film.special_features,
               'lastUpdate', film.last_update) AS "json"
          FROM dvds.film_actor
               INNER JOIN dvds.film ON (film.film_id = film_actor.film_id)
          WHERE actor.actor_id = film_actor.actor_id
          ORDER BY film.length DESC
          LIMIT ?
     )) AS "json"
FROM dvds.actor
WHERE actor.actor_id = ?;
`)

	var dest struct {
		model.Actor

		LongestFilm model.Film
	}

	err := stmt.QueryJSON(ctx, db, &dest)
	require.Nil(t, err)
	testutils.AssertJSON(t, dest, `
{
	"ActorID": 2,
	"FirstName": "NICK",
	"LastName": "WAHLBERG",
	"LastUpdate": "2006-02-15T04:34:33Z",
	"LongestFilm": {
		"FilmID": 958,
		"Title": "WARDROBE PHANTOM",
		"Description": "A Action-Packed Display of a Mad Cow And a Astronaut who must Kill a Car in Ancient India",
		"ReleaseYear": 2006,
		"LanguageID": 1,
		"OriginalLanguageID": null,
		"RentalDuration": 6,
		"RentalRate": 2.99,
		"Length": 178,
		"ReplacementCost": 19.99,
		"Rating": "G",
		"SpecialFeatures": "Trailers,Commentaries",
		"LastUpdate": "2006-02-15T05:03:42Z"
	}
}
`)

}

func TestSelectJsonArr(t *testing.T) {
	stmt := SELECT_JSON_ARR(Actor.AllColumns).
		FROM(Actor).
		ORDER_BY(Actor.ActorID)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT JSON_ARRAYAGG(JSON_OBJECT('actorID', actor.actor_id,
     'firstName', actor.first_name,
     'lastName', actor.last_name,
     'lastUpdate', actor.last_update)) AS "json"
FROM dvds.actor
ORDER BY actor.actor_id;
`)

	var dest []model.Actor

	err := stmt.QueryJSON(ctx, db, &dest)
	require.Nil(t, err)

	testutils.AssertJSONFile(t, dest, "./testdata/results/mysql/all_actors.json")
	requireLogged(t, stmt)
	requireQueryLogged(t, stmt, 1)
}

func TestSelectJsonArr_NestedArr(t *testing.T) {
	stmt := SELECT_JSON_ARR(
		Actor.AllColumns,

		SELECT_JSON_ARR(
			Film.AllColumns,
		).FROM(
			FilmActor.INNER_JOIN(
				Film,
				Film.FilmID.EQ(FilmActor.FilmID).AND(
					Actor.ActorID.EQ(FilmActor.ActorID)),
			),
		).WHERE(
			Film.FilmID.MOD(Int(17)).EQ(Int(0)),
		).ORDER_BY(
			Film.Length.DESC(),
		).AS("Films"),
	).FROM(
		Actor,
	).WHERE(
		Actor.ActorID.BETWEEN(Int(1), Int(3)),
	).ORDER_BY(
		Actor.ActorID,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT JSON_ARRAYAGG(JSON_OBJECT('actorID', actor.actor_id,
     'firstName', actor.first_name,
     'lastName', actor.last_name,
     'lastUpdate', actor.last_update,
     'Films', (
          SELECT JSON_ARRAYAGG(JSON_OBJECT('filmID', film.film_id,
               'title', film.title,
               'description', film.description,
               'releaseYear', film.release_year,
               'languageID', film.language_id,
               'originalLanguageID', film.original_language_id,
               'rentalDuration', film.rental_duration,
               'rentalRate', film.rental_rate,
               'length', film.length,
               'replacementCost', film.replacement_cost,
               'rating', film.rating,
               'specialFeatures', film.special_features,
               'lastUpdate', film.last_update)) AS "json"
          FROM dvds.film_actor
               INNER JOIN dvds.film ON ((film.film_id = film_actor.film_id) AND (actor.actor_id = film_actor.actor_id))
          WHERE (film.film_id % 17) = 0
          ORDER BY film.length DESC
     ))) AS "json"
FROM dvds.actor
WHERE actor.actor_id BETWEEN 1 AND 3
ORDER BY actor.actor_id;
`)

	var dest []struct {
		model.Actor

		Films []model.Film
	}

	err := stmt.QueryJSON(ctx, db, &dest)
	fmt.Println(err)
	require.Nil(t, err)
	testutils.AssertJSON(t, dest, `
[
	{
		"ActorID": 1,
		"FirstName": "PENELOPE",
		"LastName": "GUINESS",
		"LastUpdate": "2006-02-15T04:34:33Z",
		"Films": null
	},
	{
		"ActorID": 2,
		"FirstName": "NICK",
		"LastName": "WAHLBERG",
		"LastUpdate": "2006-02-15T04:34:33Z",
		"Films": [
			{
				"FilmID": 357,
				"Title": "GILBERT PELICAN",
				"Description": "A Fateful Tale of a Man And a Feminist who must Conquer a Crocodile in A Manhattan Penthouse",
				"ReleaseYear": 2006,
				"LanguageID": 1,
				"OriginalLanguageID": null,
				"RentalDuration": 7,
				"RentalRate": 0.99,
				"Length": 114,
				"ReplacementCost": 13.99,
				"Rating": "G",
				"SpecialFeatures": "Trailers,Commentaries",
				"LastUpdate": "2006-02-15T05:03:42Z"
			},
			{
				"FilmID": 561,
				"Title": "MASK PEACH",
				"Description": "A Boring Character Study of a Student And a Robot who must Meet a Woman in California",
				"ReleaseYear": 2006,
				"LanguageID": 1,
				"OriginalLanguageID": null,
				"RentalDuration": 6,
				"RentalRate": 2.99,
				"Length": 123,
				"ReplacementCost": 26.99,
				"Rating": "NC-17",
				"SpecialFeatures": "Commentaries,Deleted Scenes",
				"LastUpdate": "2006-02-15T05:03:42Z"
			}
		]
	},
	{
		"ActorID": 3,
		"FirstName": "ED",
		"LastName": "CHASE",
		"LastUpdate": "2006-02-15T04:34:33Z",
		"Films": [
			{
				"FilmID": 17,
				"Title": "ALONE TRIP",
				"Description": "A Fast-Paced Character Study of a Composer And a Dog who must Outgun a Boat in An Abandoned Fun House",
				"ReleaseYear": 2006,
				"LanguageID": 1,
				"OriginalLanguageID": null,
				"RentalDuration": 3,
				"RentalRate": 0.99,
				"Length": 82,
				"ReplacementCost": 14.99,
				"Rating": "R",
				"SpecialFeatures": "Trailers,Behind the Scenes",
				"LastUpdate": "2006-02-15T05:03:42Z"
			},
			{
				"FilmID": 289,
				"Title": "EVE RESURRECTION",
				"Description": "A Awe-Inspiring Yarn of a Pastry Chef And a Database Administrator who must Challenge a Teacher in A Baloon",
				"ReleaseYear": 2006,
				"LanguageID": 1,
				"OriginalLanguageID": null,
				"RentalDuration": 5,
				"RentalRate": 4.99,
				"Length": 66,
				"ReplacementCost": 25.99,
				"Rating": "G",
				"SpecialFeatures": "Trailers,Commentaries,Deleted Scenes",
				"LastUpdate": "2006-02-15T05:03:42Z"
			}
		]
	}
]
`)

}

func TestSelectJson_GroupBy(t *testing.T) {
	skipForMariaDB(t) // scope issues with select without FROM

	subQuery := SELECT(
		Customer.AllColumns,

		SUM(Payment.Amount).AS("sum"),
		AVG(Payment.Amount).AS("avg"),
		MAX(Payment.Amount).AS("max"),
		MIN(Payment.Amount).AS("min"),
		COUNT(Payment.Amount).AS("count"),
	).FROM(
		Payment.
			INNER_JOIN(Customer, Customer.CustomerID.EQ(Payment.CustomerID)),
	).GROUP_BY(
		Customer.CustomerID,
	).HAVING(
		SUMf(Payment.Amount).GT(Float(125)),
	).ORDER_BY(
		Customer.CustomerID, SUM(Payment.Amount).ASC(),
	).AsTable("customers_info")

	stmt := SELECT_JSON_ARR(
		subQuery.AllColumns().Except( // TODO: remove when ColumnList.From() is implemented
			FloatColumn("sum"),
			FloatColumn("avg"),
			FloatColumn("max"),
			FloatColumn("min"),
			FloatColumn("count"),
		),

		SELECT_JSON_OBJ(
			FloatColumn("sum").From(subQuery),
			FloatColumn("avg").From(subQuery),
			FloatColumn("max").From(subQuery),
			FloatColumn("min").From(subQuery),
			FloatColumn("count").From(subQuery),
		).AS("amount"),
	).FROM(subQuery)

	testutils.AssertDebugStatementSql(t, stmt, strings.ReplaceAll(`
SELECT JSON_ARRAYAGG(JSON_OBJECT('customerID', customers_info.""customer.customer_id"",
     'storeID', customers_info.""customer.store_id"",
     'firstName', customers_info.""customer.first_name"",
     'lastName', customers_info.""customer.last_name"",
     'email', customers_info.""customer.email"",
     'addressID', customers_info.""customer.address_id"",
     'active', customers_info.""customer.active"",
     'createDate', customers_info.""customer.create_date"",
     'lastUpdate', customers_info.""customer.last_update"",
     'amount', (
          SELECT JSON_OBJECT('sum', customers_info.sum,
               'avg', customers_info.avg,
               'max', customers_info.max,
               'min', customers_info.min,
               'count', customers_info.count) AS "json"
     ))) AS "json"
FROM (
          SELECT customer.customer_id AS "customer.customer_id",
               customer.store_id AS "customer.store_id",
               customer.first_name AS "customer.first_name",
               customer.last_name AS "customer.last_name",
               customer.email AS "customer.email",
               customer.address_id AS "customer.address_id",
               customer.active AS "customer.active",
               customer.create_date AS "customer.create_date",
               customer.last_update AS "customer.last_update",
               SUM(payment.amount) AS "sum",
               AVG(payment.amount) AS "avg",
               MAX(payment.amount) AS "max",
               MIN(payment.amount) AS "min",
               COUNT(payment.amount) AS "count"
          FROM dvds.payment
               INNER JOIN dvds.customer ON (customer.customer_id = payment.customer_id)
          GROUP BY customer.customer_id
          HAVING SUM(payment.amount) > 125
          ORDER BY customer.customer_id, SUM(payment.amount) ASC
     ) AS customers_info;
`, `""`, "`"))

	var dest []struct {
		model.Customer

		Amount struct {
			Sum   float64
			Avg   float64
			Max   float64
			Min   float64
			Count int64
		}
	}

	err := stmt.QueryJSON(ctx, db, &dest)
	fmt.Println(err)
	require.Nil(t, err)

	testutils.AssertJSONFile(t, dest, "./testdata/results/mysql/customer_payment_sum.json")
	requireLogged(t, stmt)
}

func TestSelectJsonObject_EmptyResult(t *testing.T) {

	t.Run("json obj", func(t *testing.T) {
		stmt := SELECT_JSON_OBJ(Actor.AllColumns).
			FROM(Actor).
			WHERE(Actor.FirstName.EQ(String("Kowalski")))

		var dest model.Actor

		err := stmt.QueryJSON(ctx, db, &dest)
		require.ErrorIs(t, err, qrm.ErrNoRows)
	})

	t.Run("json arr", func(t *testing.T) {
		stmt := SELECT_JSON_ARR(Actor.AllColumns).
			FROM(Actor).
			WHERE(Actor.FirstName.EQ(String("Kowalski")))

		var dest []model.Actor

		err := stmt.QueryJSON(ctx, db, &dest)
		require.NoError(t, err)
		require.Empty(t, dest)
	})
}

func TestSelectJson_ProjectionNotAliased(t *testing.T) {

	t.Run("expression not aliased", func(t *testing.T) {
		testutils.AssertPanicErr(t, func() {
			stmt := SELECT_JSON_ARR(
				Int(2).ADD(Customer.CustomerID),
			).FROM(Customer)

			stmt.DebugSql()

		}, "jet: expression need to be aliased when used as SELECT JSON projection.")
	})
}
