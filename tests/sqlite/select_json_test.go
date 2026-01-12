package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jet/jet/v2/qrm"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/internal/utils/ptr"
	. "github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/view"

	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestSelectJsonObj(t *testing.T) {
	stmt := SELECT_JSON_OBJ(Actor.AllColumns).
		FROM(Actor).
		WHERE(Actor.ActorID.EQ(Int(2)))

	testutils.AssertStatementSql(t, stmt, `
SELECT JSON_OBJECT(
          'actorID', actor.actor_id,
          'firstName', actor.first_name,
          'lastName', actor.last_name,
          'lastUpdate', strftime('%Y-%m-%dT%H:%M:%fZ', actor.last_update)
     ) AS "json"
FROM actor
WHERE actor.actor_id = ?;
`, int64(2))

	var dest model.Actor

	err := stmt.Query(db, &dest)
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
			LIMIT(1).OFFSET(3).AS("LongestFilm"),
	).FROM(
		Actor,
	).WHERE(
		Actor.ActorID.EQ(Int(2)),
	)

	testutils.AssertStatementSql(t, stmt, `
SELECT JSON_OBJECT(
          'actorID', actor.actor_id,
          'firstName', actor.first_name,
          'lastName', actor.last_name,
          'lastUpdate', strftime('%Y-%m-%dT%H:%M:%fZ', actor.last_update),
          'LongestFilm', JSON((
               SELECT JSON_OBJECT(
                         'filmID', film.film_id,
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
                         'lastUpdate', strftime('%Y-%m-%dT%H:%M:%fZ', film.last_update)
                    ) AS "json"
               FROM film_actor
                    INNER JOIN film ON (film.film_id = film_actor.film_id)
               WHERE actor.actor_id = film_actor.actor_id
               ORDER BY film.length DESC
               LIMIT ?
               OFFSET ?
          ))
     ) AS "json"
FROM actor
WHERE actor.actor_id = ?;
`)

	var dest struct {
		model.Actor

		LongestFilm model.Film
	}

	err := stmt.QueryContext(ctx, db, &dest)
	require.Nil(t, err)
	testutils.AssertJSON(t, dest, `
{
	"ActorID": 2,
	"FirstName": "NICK",
	"LastName": "WAHLBERG",
	"LastUpdate": "2019-04-11T18:11:48Z",
	"LongestFilm": {
		"FilmID": 754,
		"Title": "RUSHMORE MERMAID",
		"Description": "A Boring Story of a Woman And a Moose who must Reach a Husband in A Shark Tank",
		"ReleaseYear": "2006",
		"LanguageID": 1,
		"OriginalLanguageID": null,
		"RentalDuration": 6,
		"RentalRate": 2.99,
		"Length": 150,
		"ReplacementCost": 17.99,
		"Rating": "PG-13",
		"SpecialFeatures": "Trailers,Commentaries,Deleted Scenes",
		"LastUpdate": "2019-04-11T18:11:48Z"
	}
}
`)

}

func TestSelectJsonArr(t *testing.T) {
	stmt := SELECT_JSON_ARR(Actor.AllColumns).
		FROM(Actor).
		ORDER_BY(Actor.ActorID)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT JSON_GROUP_ARRAY(JSON_OBJECT(
          'actorID', actor.actor_id,
          'firstName', actor.first_name,
          'lastName', actor.last_name,
          'lastUpdate', strftime('%Y-%m-%dT%H:%M:%fZ', actor.last_update)
     )) AS "json"
FROM actor
ORDER BY actor.actor_id;
`)

	var dest []model.Actor

	err := stmt.Query(db, &dest)
	require.Nil(t, err)

	testutils.AssertJSONFile(t, dest, "./testdata/results/sqlite/all_actors.json")
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
SELECT JSON_GROUP_ARRAY(JSON_OBJECT(
          'actorID', actor.actor_id,
          'firstName', actor.first_name,
          'lastName', actor.last_name,
          'lastUpdate', strftime('%Y-%m-%dT%H:%M:%fZ', actor.last_update),
          'Films', (
               SELECT JSON_GROUP_ARRAY(JSON_OBJECT(
                         'filmID', film.film_id,
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
                         'lastUpdate', strftime('%Y-%m-%dT%H:%M:%fZ', film.last_update)
                    )) AS "json"
               FROM film_actor
                    INNER JOIN film ON ((film.film_id = film_actor.film_id) AND (actor.actor_id = film_actor.actor_id))
               WHERE (film.film_id % 17) = 0
               ORDER BY film.length DESC
          )
     )) AS "json"
FROM actor
WHERE actor.actor_id BETWEEN 1 AND 3
ORDER BY actor.actor_id;
`)

	var dest []struct {
		model.Actor

		Films []model.Film
	}

	err := stmt.QueryContext(ctx, db, &dest)
	require.Nil(t, err)
	testutils.AssertJSON(t, dest, `
[
	{
		"ActorID": 1,
		"FirstName": "PENELOPE",
		"LastName": "GUINESS",
		"LastUpdate": "2019-04-11T18:11:48Z",
		"Films": []
	},
	{
		"ActorID": 2,
		"FirstName": "NICK",
		"LastName": "WAHLBERG",
		"LastUpdate": "2019-04-11T18:11:48Z",
		"Films": [
			{
				"FilmID": 357,
				"Title": "GILBERT PELICAN",
				"Description": "A Fateful Tale of a Man And a Feminist who must Conquer a Crocodile in A Manhattan Penthouse",
				"ReleaseYear": "2006",
				"LanguageID": 1,
				"OriginalLanguageID": null,
				"RentalDuration": 7,
				"RentalRate": 0.99,
				"Length": 114,
				"ReplacementCost": 13.99,
				"Rating": "G",
				"SpecialFeatures": "Trailers,Commentaries",
				"LastUpdate": "2019-04-11T18:11:48Z"
			},
			{
				"FilmID": 561,
				"Title": "MASK PEACH",
				"Description": "A Boring Character Study of a Student And a Robot who must Meet a Woman in California",
				"ReleaseYear": "2006",
				"LanguageID": 1,
				"OriginalLanguageID": null,
				"RentalDuration": 6,
				"RentalRate": 2.99,
				"Length": 123,
				"ReplacementCost": 26.99,
				"Rating": "NC-17",
				"SpecialFeatures": "Commentaries,Deleted Scenes",
				"LastUpdate": "2019-04-11T18:11:48Z"
			}
		]
	},
	{
		"ActorID": 3,
		"FirstName": "ED",
		"LastName": "CHASE",
		"LastUpdate": "2019-04-11T18:11:48Z",
		"Films": [
			{
				"FilmID": 17,
				"Title": "ALONE TRIP",
				"Description": "A Fast-Paced Character Study of a Composer And a Dog who must Outgun a Boat in An Abandoned Fun House",
				"ReleaseYear": "2006",
				"LanguageID": 1,
				"OriginalLanguageID": null,
				"RentalDuration": 3,
				"RentalRate": 0.99,
				"Length": 82,
				"ReplacementCost": 14.99,
				"Rating": "R",
				"SpecialFeatures": "Trailers,Behind the Scenes",
				"LastUpdate": "2019-04-11T18:11:48Z"
			},
			{
				"FilmID": 289,
				"Title": "EVE RESURRECTION",
				"Description": "A Awe-Inspiring Yarn of a Pastry Chef And a Database Administrator who must Challenge a Teacher in A Baloon",
				"ReleaseYear": "2006",
				"LanguageID": 1,
				"OriginalLanguageID": null,
				"RentalDuration": 5,
				"RentalRate": 4.99,
				"Length": 66,
				"ReplacementCost": 25.99,
				"Rating": "G",
				"SpecialFeatures": "Trailers,Commentaries,Deleted Scenes",
				"LastUpdate": "2019-04-11T18:11:48Z"
			}
		]
	}
]
`)

}

func TestSelectJson_GroupBy(t *testing.T) {

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
		Customer.AllColumns.From(subQuery),

		SELECT_JSON_OBJ(
			FloatColumn("sum").From(subQuery),
			FloatColumn("avg").From(subQuery),
			FloatColumn("max").From(subQuery),
			FloatColumn("min").From(subQuery),
			FloatColumn("count").From(subQuery),
		).AS("amount"),
	).FROM(subQuery)

	testutils.AssertDebugStatementSql(t, stmt, strings.ReplaceAll(`
SELECT JSON_GROUP_ARRAY(JSON_OBJECT(
          'customerID', customers_info.''customer.customer_id'',
          'storeID', customers_info.''customer.store_id'',
          'firstName', customers_info.''customer.first_name'',
          'lastName', customers_info.''customer.last_name'',
          'email', customers_info.''customer.email'',
          'addressID', customers_info.''customer.address_id'',
          'active', customers_info.''customer.active'',
          'createDate', strftime('%Y-%m-%dT%H:%M:%fZ', customers_info.''customer.create_date''),
          'lastUpdate', strftime('%Y-%m-%dT%H:%M:%fZ', customers_info.''customer.last_update''),
          'amount', JSON((
               SELECT JSON_OBJECT(
                         'sum', customers_info.sum,
                         'avg', customers_info.avg,
                         'max', customers_info.max,
                         'min', customers_info.min,
                         'count', customers_info.count
                    ) AS "json"
          ))
     )) AS "json"
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
          FROM payment
               INNER JOIN customer ON (customer.customer_id = payment.customer_id)
          GROUP BY customer.customer_id
          HAVING SUM(payment.amount) > 125
          ORDER BY customer.customer_id, SUM(payment.amount) ASC
     ) AS customers_info;
`, "''", "`"))

	type Dest []struct {
		model.Customer

		Amount struct {
			Sum   float64
			Avg   float64
			Max   float64
			Min   float64
			Count int64
		}
	}

	var actual Dest

	err := stmt.QueryContext(ctx, db, &actual)
	require.Nil(t, err)

	fullpath, _ := filepath.Abs("../testdata/results/sqlite/customer_payment_sum.json")
	expectedData, err := os.ReadFile(fullpath)
	require.Nil(t, err)

	var expected Dest
	err = json.Unmarshal(expectedData, &expected)
	require.Nil(t, err)

	testutils.AssertDeepEqual(t, actual, expected, cmpopts.EquateApprox(0.0, 1e-9))

	// testutils.AssertJSONFile(t, dest, "./testdata/results/sqlite/customer_payment_sum.json")
	requireLogged(t, stmt)
}

func TestSelectJsonObject_EmptyResult(t *testing.T) {

	t.Run("json obj", func(t *testing.T) {
		stmt := SELECT_JSON_OBJ(Actor.AllColumns).
			FROM(Actor).
			WHERE(Actor.FirstName.EQ(String("Kowalski")))

		var dest model.Actor

		err := stmt.QueryContext(ctx, db, &dest)
		require.ErrorIs(t, err, qrm.ErrNoRows)
	})

	t.Run("json arr", func(t *testing.T) {
		stmt := SELECT_JSON_ARR(Actor.AllColumns).
			FROM(Actor).
			WHERE(Actor.FirstName.EQ(String("Kowalski")))

		var dest []model.Actor

		err := stmt.QueryContext(ctx, db, &dest)
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

func TestSelectJsonInReturning(t *testing.T) {
	stmt := Rental.
		UPDATE(Rental.ReturnDate).
		MODEL(model.Rental{
			ReturnDate: ptr.Of(time.Date(2010, 2, 4, 5, 6, 7, 8, time.UTC)),
		}).
		WHERE(
			Rental.RentalID.EQ(Int(11496)),
		).
		RETURNING(
			Rental.AllColumns.Except(Rental.LastUpdate),

			SELECT_JSON_OBJ(
				Customer.AllColumns,
			).FROM(
				Customer,
			).WHERE(
				Customer.CustomerID.EQ(Rental.CustomerID),
			).AS("Customer"),
		)

	testutils.AssertStatementSql(t, stmt, `
UPDATE rental
SET return_date = ?
WHERE rental.rental_id = ?
RETURNING rental.rental_id AS "rental.rental_id",
          rental.rental_date AS "rental.rental_date",
          rental.inventory_id AS "rental.inventory_id",
          rental.customer_id AS "rental.customer_id",
          rental.return_date AS "rental.return_date",
          rental.staff_id AS "rental.staff_id",
          JSON((
               SELECT JSON_OBJECT(
                         'customerID', customer.customer_id,
                         'storeID', customer.store_id,
                         'firstName', customer.first_name,
                         'lastName', customer.last_name,
                         'email', customer.email,
                         'addressID', customer.address_id,
                         'active', customer.active,
                         'createDate', strftime('%Y-%m-%dT%H:%M:%fZ', customer.create_date),
                         'lastUpdate', strftime('%Y-%m-%dT%H:%M:%fZ', customer.last_update)
                    ) AS "json"
               FROM customer
               WHERE customer.customer_id = rental.customer_id
          )) AS "Customer";
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var dest struct {
			model.Rental

			Customer model.Customer `json_column:"Customer"`
		}

		err := stmt.Query(tx, &dest)
		require.NoError(t, err)
		testutils.AssertJSON(t, dest, `
{
	"RentalID": 11496,
	"RentalDate": "2006-02-14T15:16:03Z",
	"InventoryID": 2047,
	"CustomerID": 155,
	"ReturnDate": "2010-02-04T05:06:07.000000008Z",
	"StaffID": 1,
	"LastUpdate": "0001-01-01T00:00:00Z",
	"Customer": {
		"CustomerID": 155,
		"StoreID": 1,
		"FirstName": "GAIL",
		"LastName": "KNIGHT",
		"Email": "GAIL.KNIGHT@sakilacustomer.org",
		"AddressID": 159,
		"Active": "1",
		"CreateDate": "2006-02-14T22:04:36Z",
		"LastUpdate": "2019-04-11T18:11:49Z"
	}
}
`)
	})
}

func TestSelectJson_QueryWithoutUnMarshaling(t *testing.T) {
	stmt := SELECT(
		SELECT_JSON_ARR(
			view.CustomerList.AllColumns,

			SELECT_JSON_ARR(Rental.AllColumns).
				FROM(Rental).
				WHERE(view.CustomerList.ID.EQ(Rental.CustomerID)).
				ORDER_BY(Rental.CustomerID).
				AS("Rentals"),
		).FROM(
			view.CustomerList,
		).WHERE(
			view.CustomerList.ID.LT_EQ(Int(2)),
		).ORDER_BY(
			view.CustomerList.ID,
		).AS("raw_json"),
	)

	fmt.Println(stmt.DebugSql())

	testutils.AssertDebugStatementSql(t, stmt, strings.ReplaceAll(`
SELECT (
          SELECT JSON_GROUP_ARRAY(JSON_OBJECT(
                    'iD', customer_list.''ID'',
                    'name', customer_list.name,
                    'address', customer_list.address,
                    'zipCode', customer_list.zip_code,
                    'phone', customer_list.phone,
                    'city', customer_list.city,
                    'country', customer_list.country,
                    'notes', customer_list.notes,
                    'sID', customer_list.''SID'',
                    'Rentals', (
                         SELECT JSON_GROUP_ARRAY(JSON_OBJECT(
                                   'rentalID', rental.rental_id,
                                   'rentalDate', strftime('%Y-%m-%dT%H:%M:%fZ', rental.rental_date),
                                   'inventoryID', rental.inventory_id,
                                   'customerID', rental.customer_id,
                                   'returnDate', strftime('%Y-%m-%dT%H:%M:%fZ', rental.return_date),
                                   'staffID', rental.staff_id,
                                   'lastUpdate', strftime('%Y-%m-%dT%H:%M:%fZ', rental.last_update)
                              )) AS "json"
                         FROM rental
                         WHERE customer_list.''ID'' = rental.customer_id
                         ORDER BY rental.customer_id
                    )
               )) AS "json"
          FROM customer_list
          WHERE customer_list.''ID'' <= 2
          ORDER BY customer_list.''ID''
     ) AS "raw_json";
`, "''", "`"))

	var dest struct {
		RawJson []byte
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	require.Equal(t, string(dest.RawJson), `[{"iD":1,"name":"MARY SMITH","address":"1913 Hanoi Way","zipCode":"35200","phone":" ","city":"Sasebo","country":"Japan","notes":"active","sID":1,"Rentals":[{"rentalID":76,"rentalDate":"2005-05-25T11:30:37.000Z","inventoryID":3021,"customerID":1,"returnDate":"2005-06-03T12:00:37.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":573,"rentalDate":"2005-05-28T10:35:23.000Z","inventoryID":4020,"customerID":1,"returnDate":"2005-06-03T06:32:23.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":1185,"rentalDate":"2005-06-15T00:54:12.000Z","inventoryID":2785,"customerID":1,"returnDate":"2005-06-23T02:42:12.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":1422,"rentalDate":"2005-06-15T18:02:53.000Z","inventoryID":1021,"customerID":1,"returnDate":"2005-06-19T15:54:53.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":1476,"rentalDate":"2005-06-15T21:08:46.000Z","inventoryID":1407,"customerID":1,"returnDate":"2005-06-25T02:26:46.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":1725,"rentalDate":"2005-06-16T15:18:57.000Z","inventoryID":726,"customerID":1,"returnDate":"2005-06-17T21:05:57.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":2308,"rentalDate":"2005-06-18T08:41:48.000Z","inventoryID":197,"customerID":1,"returnDate":"2005-06-22T03:36:48.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":2363,"rentalDate":"2005-06-18T13:33:59.000Z","inventoryID":3497,"customerID":1,"returnDate":"2005-06-19T17:40:59.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":3284,"rentalDate":"2005-06-21T06:24:45.000Z","inventoryID":4566,"customerID":1,"returnDate":"2005-06-28T03:28:45.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":4526,"rentalDate":"2005-07-08T03:17:05.000Z","inventoryID":1443,"customerID":1,"returnDate":"2005-07-14T01:19:05.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":4611,"rentalDate":"2005-07-08T07:33:56.000Z","inventoryID":3486,"customerID":1,"returnDate":"2005-07-12T13:25:56.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":5244,"rentalDate":"2005-07-09T13:24:07.000Z","inventoryID":3726,"customerID":1,"returnDate":"2005-07-14T14:01:07.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":5326,"rentalDate":"2005-07-09T16:38:01.000Z","inventoryID":797,"customerID":1,"returnDate":"2005-07-13T18:02:01.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":6163,"rentalDate":"2005-07-11T10:13:46.000Z","inventoryID":1330,"customerID":1,"returnDate":"2005-07-19T13:15:46.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":7273,"rentalDate":"2005-07-27T11:31:22.000Z","inventoryID":2465,"customerID":1,"returnDate":"2005-07-31T06:50:22.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":7841,"rentalDate":"2005-07-28T09:04:45.000Z","inventoryID":1092,"customerID":1,"returnDate":"2005-07-30T12:37:45.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":8033,"rentalDate":"2005-07-28T16:18:23.000Z","inventoryID":4268,"customerID":1,"returnDate":"2005-07-30T17:56:23.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":8074,"rentalDate":"2005-07-28T17:33:39.000Z","inventoryID":1558,"customerID":1,"returnDate":"2005-07-29T20:17:39.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":8116,"rentalDate":"2005-07-28T19:20:07.000Z","inventoryID":4497,"customerID":1,"returnDate":"2005-07-29T22:54:07.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":8326,"rentalDate":"2005-07-29T03:58:49.000Z","inventoryID":108,"customerID":1,"returnDate":"2005-08-01T05:16:49.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":9571,"rentalDate":"2005-07-31T02:42:18.000Z","inventoryID":2219,"customerID":1,"returnDate":"2005-08-02T23:26:18.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":10437,"rentalDate":"2005-08-01T08:51:04.000Z","inventoryID":14,"customerID":1,"returnDate":"2005-08-10T12:12:04.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":11299,"rentalDate":"2005-08-02T15:36:52.000Z","inventoryID":3232,"customerID":1,"returnDate":"2005-08-10T16:40:52.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":11367,"rentalDate":"2005-08-02T18:01:38.000Z","inventoryID":1440,"customerID":1,"returnDate":"2005-08-04T13:19:38.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":11824,"rentalDate":"2005-08-17T12:37:54.000Z","inventoryID":2639,"customerID":1,"returnDate":"2005-08-19T10:11:54.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":12250,"rentalDate":"2005-08-18T03:57:29.000Z","inventoryID":921,"customerID":1,"returnDate":"2005-08-22T23:05:29.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":13068,"rentalDate":"2005-08-19T09:55:16.000Z","inventoryID":3019,"customerID":1,"returnDate":"2005-08-20T14:44:16.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":13176,"rentalDate":"2005-08-19T13:56:54.000Z","inventoryID":2269,"customerID":1,"returnDate":"2005-08-23T08:50:54.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":14762,"rentalDate":"2005-08-21T23:33:57.000Z","inventoryID":4249,"customerID":1,"returnDate":"2005-08-23T01:30:57.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:50.000Z"},{"rentalID":14825,"rentalDate":"2005-08-22T01:27:57.000Z","inventoryID":1449,"customerID":1,"returnDate":"2005-08-27T07:01:57.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:50.000Z"},{"rentalID":15298,"rentalDate":"2005-08-22T19:41:37.000Z","inventoryID":1446,"customerID":1,"returnDate":"2005-08-28T22:49:37.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:50.000Z"},{"rentalID":15315,"rentalDate":"2005-08-22T20:03:46.000Z","inventoryID":312,"customerID":1,"returnDate":"2005-08-30T01:51:46.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:50.000Z"}]},{"iD":2,"name":"PATRICIA JOHNSON","address":"1121 Loja Avenue","zipCode":"17886","phone":" ","city":"San Bernardino","country":"United States","notes":"active","sID":1,"Rentals":[{"rentalID":320,"rentalDate":"2005-05-27T00:09:24.000Z","inventoryID":1090,"customerID":2,"returnDate":"2005-05-28T04:30:24.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":2128,"rentalDate":"2005-06-17T20:54:58.000Z","inventoryID":352,"customerID":2,"returnDate":"2005-06-24T00:41:58.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":5636,"rentalDate":"2005-07-10T06:31:24.000Z","inventoryID":4116,"customerID":2,"returnDate":"2005-07-13T02:36:24.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":5755,"rentalDate":"2005-07-10T12:38:56.000Z","inventoryID":2760,"customerID":2,"returnDate":"2005-07-19T17:02:56.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":7346,"rentalDate":"2005-07-27T14:30:42.000Z","inventoryID":741,"customerID":2,"returnDate":"2005-08-02T16:48:42.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":7376,"rentalDate":"2005-07-27T15:23:02.000Z","inventoryID":488,"customerID":2,"returnDate":"2005-08-04T10:35:02.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":7459,"rentalDate":"2005-07-27T18:40:20.000Z","inventoryID":2053,"customerID":2,"returnDate":"2005-08-02T21:07:20.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":8230,"rentalDate":"2005-07-29T00:12:59.000Z","inventoryID":1937,"customerID":2,"returnDate":"2005-08-06T19:52:59.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":8598,"rentalDate":"2005-07-29T12:56:59.000Z","inventoryID":626,"customerID":2,"returnDate":"2005-08-01T08:39:59.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":8705,"rentalDate":"2005-07-29T17:14:29.000Z","inventoryID":4038,"customerID":2,"returnDate":"2005-08-02T16:01:29.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":9031,"rentalDate":"2005-07-30T06:06:10.000Z","inventoryID":2377,"customerID":2,"returnDate":"2005-08-04T10:45:10.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":9236,"rentalDate":"2005-07-30T13:47:43.000Z","inventoryID":4030,"customerID":2,"returnDate":"2005-08-08T18:52:43.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":9248,"rentalDate":"2005-07-30T14:14:11.000Z","inventoryID":1382,"customerID":2,"returnDate":"2005-08-05T11:19:11.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":9296,"rentalDate":"2005-07-30T16:21:13.000Z","inventoryID":4088,"customerID":2,"returnDate":"2005-08-08T11:57:13.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":9465,"rentalDate":"2005-07-30T22:39:53.000Z","inventoryID":3084,"customerID":2,"returnDate":"2005-08-06T16:43:53.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":10136,"rentalDate":"2005-07-31T21:58:56.000Z","inventoryID":3142,"customerID":2,"returnDate":"2005-08-03T19:44:56.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":10466,"rentalDate":"2005-08-01T09:45:26.000Z","inventoryID":138,"customerID":2,"returnDate":"2005-08-06T06:28:26.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":10918,"rentalDate":"2005-08-02T02:10:56.000Z","inventoryID":3418,"customerID":2,"returnDate":"2005-08-02T21:23:56.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":11087,"rentalDate":"2005-08-02T07:41:41.000Z","inventoryID":654,"customerID":2,"returnDate":"2005-08-10T10:37:41.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":11177,"rentalDate":"2005-08-02T10:43:48.000Z","inventoryID":1149,"customerID":2,"returnDate":"2005-08-10T10:55:48.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":11256,"rentalDate":"2005-08-02T13:44:53.000Z","inventoryID":2060,"customerID":2,"returnDate":"2005-08-04T16:39:53.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":11614,"rentalDate":"2005-08-17T03:52:18.000Z","inventoryID":805,"customerID":2,"returnDate":"2005-08-20T07:04:18.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":12963,"rentalDate":"2005-08-19T06:26:04.000Z","inventoryID":1521,"customerID":2,"returnDate":"2005-08-23T11:37:04.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:49.000Z"},{"rentalID":14475,"rentalDate":"2005-08-21T13:24:32.000Z","inventoryID":3164,"customerID":2,"returnDate":"2005-08-27T08:59:32.000Z","staffID":2,"lastUpdate":"2019-04-11T18:11:50.000Z"},{"rentalID":14743,"rentalDate":"2005-08-21T22:41:56.000Z","inventoryID":4570,"customerID":2,"returnDate":"2005-08-29T00:18:56.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:50.000Z"},{"rentalID":15145,"rentalDate":"2005-08-22T13:53:04.000Z","inventoryID":2179,"customerID":2,"returnDate":"2005-08-31T15:51:04.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:50.000Z"},{"rentalID":15907,"rentalDate":"2005-08-23T17:39:35.000Z","inventoryID":2898,"customerID":2,"returnDate":"2005-08-25T23:23:35.000Z","staffID":1,"lastUpdate":"2019-04-11T18:11:50.000Z"}]}]`)

}
