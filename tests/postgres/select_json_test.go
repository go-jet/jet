package postgres

import (
	"testing"
	"time"

	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/internal/utils/ptr"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/view"
	"github.com/stretchr/testify/require"

	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/table"
)

func TestSelectJsonObject(t *testing.T) {
	stmt := SELECT_JSON_OBJ(Actor.AllColumns).
		FROM(Actor).
		WHERE(Actor.ActorID.EQ(Int32(2)))

	testutils.AssertStatementSql(t, stmt, `
SELECT row_to_json(records) AS "json"
FROM (
          SELECT actor.actor_id AS "actorID",
               actor.first_name AS "firstName",
               actor.last_name AS "lastName",
               to_char(actor.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
          FROM dvds.actor
          WHERE actor.actor_id = $1::integer
     ) AS records;
`, int32(2))

	var dest model.Actor

	err := stmt.QueryContext(ctx, db, &dest)
	require.NoError(t, err)
	testutils.AssertJsonEqual(t, dest, actor2)
	requireLogged(t, stmt)

	t.Run("scan to map", func(t *testing.T) {
		var dest2 map[string]interface{}

		err = stmt.QueryContext(ctx, db, &dest2)
		require.NoError(t, err)
		//testutils.PrintJson(dest2)
		testutils.AssertDeepEqual(t, dest2, map[string]interface{}{
			"actorID":    float64(2),
			"firstName":  "Nick",
			"lastName":   "Wahlberg",
			"lastUpdate": "2013-05-26T14:47:57.620000Z",
		})
	})
}

func TestSelectJsonArr(t *testing.T) {
	stmt := SELECT_JSON_ARR(
		Rental.StaffID,
		Rental.CustomerID,
		Rental.RentalID,
	).DISTINCT(
		Rental.StaffID,
		Rental.CustomerID,
	).FROM(
		Rental,
	).WHERE(
		Rental.CustomerID.LT(Int(2)),
	).ORDER_BY(
		Rental.StaffID.ASC(),
		Rental.CustomerID.ASC(),
		Rental.RentalID.ASC(),
	)

	testutils.AssertStatementSql(t, stmt, `
SELECT json_agg(row_to_json(records)) AS "json"
FROM (
          SELECT DISTINCT ON (rental.staff_id, rental.customer_id) rental.staff_id AS "staffID",
               rental.customer_id AS "customerID",
               rental.rental_id AS "rentalID"
          FROM dvds.rental
          WHERE rental.customer_id < $1
          ORDER BY rental.staff_id ASC, rental.customer_id ASC, rental.rental_id ASC
     ) AS records;
`, int64(2))

	var dest []model.Rental

	err := stmt.QueryContext(ctx, db, &dest)
	require.NoError(t, err)
	testutils.AssertJSON(t, dest, `
[
	{
		"RentalID": 573,
		"RentalDate": "0001-01-01T00:00:00Z",
		"InventoryID": 0,
		"CustomerID": 1,
		"ReturnDate": null,
		"StaffID": 1,
		"LastUpdate": "0001-01-01T00:00:00Z"
	},
	{
		"RentalID": 76,
		"RentalDate": "0001-01-01T00:00:00Z",
		"InventoryID": 0,
		"CustomerID": 1,
		"ReturnDate": null,
		"StaffID": 2,
		"LastUpdate": "0001-01-01T00:00:00Z"
	}
]
`)
	t.Run("scan to array map", func(t *testing.T) {
		var dest2 []map[string]interface{}

		err := stmt.QueryContext(ctx, db, &dest2)
		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest2, []map[string]interface{}{
			{
				"rentalID":   573.,
				"customerID": 1.,
				"staffID":    1.,
			},
			{
				"rentalID":   76.,
				"customerID": 1.,
				"staffID":    2.,
			},
		})
	})
}

func TestSelectJsonArr_NestedArr(t *testing.T) {

	stmt := SELECT_JSON_ARR(
		Customer.AllColumns,

		SELECT_JSON_ARR(Rental.AllColumns).
			FROM(Rental).
			WHERE(Rental.CustomerID.EQ(Customer.CustomerID)).
			ORDER_BY(Rental.RentalID).
			OFFSET_e(Int(1)).LIMIT(3).AS("Rentals"),
	).FROM(
		Customer,
	).ORDER_BY(
		Customer.CustomerID,
	).LIMIT(2).OFFSET(1)

	testutils.AssertStatementSql(t, stmt, `
SELECT json_agg(row_to_json(records)) AS "json"
FROM (
          SELECT customer.customer_id AS "customerID",
               customer.store_id AS "storeID",
               customer.first_name AS "firstName",
               customer.last_name AS "lastName",
               customer.email AS "email",
               customer.address_id AS "addressID",
               customer.activebool AS "activebool",
               (to_char(customer.create_date::timestamp, 'YYYY-MM-DD') || 'T00:00:00Z') AS "createDate",
               to_char(customer.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate",
               customer.active AS "active",
               (
                    SELECT json_agg(row_to_json(rentals_records)) AS "rentals_json"
                    FROM (
                              SELECT rental.rental_id AS "rentalID",
                                   to_char(rental.rental_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "rentalDate",
                                   rental.inventory_id AS "inventoryID",
                                   rental.customer_id AS "customerID",
                                   to_char(rental.return_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "returnDate",
                                   rental.staff_id AS "staffID",
                                   to_char(rental.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
                              FROM dvds.rental
                              WHERE rental.customer_id = customer.customer_id
                              ORDER BY rental.rental_id
                              LIMIT $1
                              OFFSET $2
                         ) AS rentals_records
               ) AS "Rentals"
          FROM dvds.customer
          ORDER BY customer.customer_id
          LIMIT $3
          OFFSET $4
     ) AS records;
`)

	var dest []struct {
		model.Customer

		Rentals []model.Rental
	}

	err := stmt.QueryContext(ctx, db, &dest)
	require.NoError(t, err)

	t.Run("partial select json", func(t *testing.T) {

		stmt := SELECT(
			Customer.AllColumns,

			SELECT_JSON_ARR(Rental.AllColumns).
				FROM(Rental).
				WHERE(Rental.CustomerID.EQ(Customer.CustomerID)).
				ORDER_BY(Rental.RentalID).
				OFFSET_e(Int(1)).LIMIT(3).AS("Rentals"),
		).FROM(
			Customer,
		).ORDER_BY(
			Customer.CustomerID,
		).OFFSET(1).LIMIT(2)

		testutils.AssertStatementSql(t, stmt, `
SELECT customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.activebool AS "customer.activebool",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     customer.active AS "customer.active",
     (
          SELECT json_agg(row_to_json(rentals_records)) AS "rentals_json"
          FROM (
                    SELECT rental.rental_id AS "rentalID",
                         to_char(rental.rental_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "rentalDate",
                         rental.inventory_id AS "inventoryID",
                         rental.customer_id AS "customerID",
                         to_char(rental.return_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "returnDate",
                         rental.staff_id AS "staffID",
                         to_char(rental.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
                    FROM dvds.rental
                    WHERE rental.customer_id = customer.customer_id
                    ORDER BY rental.rental_id
                    LIMIT $1
                    OFFSET $2
               ) AS rentals_records
     ) AS "Rentals"
FROM dvds.customer
ORDER BY customer.customer_id
LIMIT $3
OFFSET $4;
`)

		var dest2 []struct {
			model.Customer

			Rentals []model.Rental `json_column:"Rentals"`
		}

		err := stmt.Query(db, &dest2)
		require.NoError(t, err)
		testutils.AssertJsonEqual(t, dest, dest2)

		var dest3 []struct {
			model.Customer

			Rentals *[]model.Rental `json_column:"rentals"`
		}

		err = stmt.Query(db, &dest3)
		require.NoError(t, err)
		testutils.AssertJsonEqual(t, dest, dest3)

		var dest4 []struct {
			model.Customer

			Rentals []*model.Rental `json_column:"rentals"`
		}

		err = stmt.Query(db, &dest4)
		require.NoError(t, err)
		testutils.AssertJsonEqual(t, dest, dest4)
	})
}

func TestSelectJson_GroupByHaving(t *testing.T) {
	stmt := SELECT_JSON_ARR(
		Customer.AllColumns,

		SELECT_JSON_OBJ(
			SUM(Payment.Amount).AS("sum"),
			AVG(Payment.Amount).AS("avg"),
			MAX(Payment.PaymentDate).AS("max_date"),
			MAX(Payment.Amount).AS("max"),
			MIN(Payment.PaymentDate).AS("min_date"),
			MIN(Payment.Amount).AS("min"),
			COUNT(Payment.Amount).AS("count"),
		).AS("amount"),
	).FROM(
		Payment.
			INNER_JOIN(Customer, Customer.CustomerID.EQ(Payment.CustomerID)),
	).GROUP_BY(
		Customer.CustomerID,
	).HAVING(
		SUMf(Payment.Amount).GT(Real(125)),
	).ORDER_BY(
		Customer.CustomerID, SUM(Payment.Amount).ASC(),
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT json_agg(row_to_json(records)) AS "json"
FROM (
          SELECT customer.customer_id AS "customerID",
               customer.store_id AS "storeID",
               customer.first_name AS "firstName",
               customer.last_name AS "lastName",
               customer.email AS "email",
               customer.address_id AS "addressID",
               customer.activebool AS "activebool",
               (to_char(customer.create_date::timestamp, 'YYYY-MM-DD') || 'T00:00:00Z') AS "createDate",
               to_char(customer.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate",
               customer.active AS "active",
               (
                    SELECT row_to_json(amount_records) AS "amount_json"
                    FROM (
                              SELECT SUM(payment.amount) AS "sum",
                                   AVG(payment.amount) AS "avg",
                                   MAX(payment.payment_date) AS "max_date",
                                   MAX(payment.amount) AS "max",
                                   MIN(payment.payment_date) AS "min_date",
                                   MIN(payment.amount) AS "min",
                                   COUNT(payment.amount) AS "count"
                         ) AS amount_records
               ) AS "amount"
          FROM dvds.payment
               INNER JOIN dvds.customer ON (customer.customer_id = payment.customer_id)
          GROUP BY customer.customer_id
          HAVING SUM(payment.amount) > 125::real
          ORDER BY customer.customer_id, SUM(payment.amount) ASC
     ) AS records;
`)

	var dest []struct {
		model.Customer

		Amount struct {
			Sum   float64
			Avg   float64
			Max   float64
			Min   float64
			Count int64
		} `alias:"amount"`
	}

	err := stmt.QueryContext(ctx, db, &dest)
	require.NoError(t, err)

	if sourceIsCockroachDB() {
		return // small precision difference in result
	}

	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/customer_payment_sum.json")
}

func TestSelectQuickStartJSON(t *testing.T) {

	stmt := SELECT_JSON_ARR(
		Actor.ActorID, Actor.FirstName, Actor.LastName, Actor.LastUpdate,

		SELECT_JSON_ARR(
			Film.AllColumns,

			SELECT_JSON_OBJ(
				Language.AllColumns,
			).FROM(
				Language,
			).WHERE(
				Language.LanguageID.EQ(Film.LanguageID).AND(
					Language.Name.EQ(Char(20)("English")),
				),
			).AS("Language"),

			SELECT_JSON_ARR(
				Category.AllColumns,
			).FROM(
				Category.
					INNER_JOIN(FilmCategory, FilmCategory.CategoryID.EQ(Category.CategoryID)),
			).WHERE(
				FilmCategory.FilmID.EQ(Film.FilmID).AND(
					Category.Name.NOT_EQ(Text("Action")),
				),
			).AS("Categories"),
		).FROM(
			Film.
				INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(Film.FilmID)),
		).WHERE(
			AND(
				FilmActor.ActorID.EQ(Actor.ActorID),
				Film.Length.GT(Int32(180)),
				String("Trailers").EQ(ANY(Film.SpecialFeatures)),
			),
		).ORDER_BY(
			Film.FilmID.ASC(),
		).AS("Films"),
	).FROM(
		Actor,
	).ORDER_BY(
		Actor.ActorID.ASC(),
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT json_agg(row_to_json(records)) AS "json"
FROM (
          SELECT actor.actor_id AS "actorID",
               actor.first_name AS "firstName",
               actor.last_name AS "lastName",
               to_char(actor.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate",
               (
                    SELECT json_agg(row_to_json(films_records)) AS "films_json"
                    FROM (
                              SELECT film.film_id AS "filmID",
                                   film.title AS "title",
                                   film.description AS "description",
                                   film.release_year AS "releaseYear",
                                   film.language_id AS "languageID",
                                   film.rental_duration AS "rentalDuration",
                                   film.rental_rate AS "rentalRate",
                                   film.length AS "length",
                                   film.replacement_cost AS "replacementCost",
                                   film.rating AS "rating",
                                   to_char(film.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate",
                                   film.special_features AS "specialFeatures",
                                   film.fulltext AS "fulltext",
                                   (
                                        SELECT row_to_json(language_records) AS "language_json"
                                        FROM (
                                                  SELECT language.language_id AS "languageID",
                                                       language.name AS "name",
                                                       to_char(language.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
                                                  FROM dvds.language
                                                  WHERE (language.language_id = film.language_id) AND (language.name = 'English'::char(20))
                                             ) AS language_records
                                   ) AS "Language",
                                   (
                                        SELECT json_agg(row_to_json(categories_records)) AS "categories_json"
                                        FROM (
                                                  SELECT category.category_id AS "categoryID",
                                                       category.name AS "name",
                                                       to_char(category.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
                                                  FROM dvds.category
                                                       INNER JOIN dvds.film_category ON (film_category.category_id = category.category_id)
                                                  WHERE (film_category.film_id = film.film_id) AND (category.name != 'Action'::text)
                                             ) AS categories_records
                                   ) AS "Categories"
                              FROM dvds.film
                                   INNER JOIN dvds.film_actor ON (film_actor.film_id = film.film_id)
                              WHERE (
                                        (film_actor.actor_id = actor.actor_id)
                                            AND (film.length > 180::integer)
                                            AND ('Trailers'::text = ANY(film.special_features))
                                    )
                              ORDER BY film.film_id ASC
                         ) AS films_records
               ) AS "Films"
          FROM dvds.actor
          ORDER BY actor.actor_id ASC
     ) AS records;
`)

	var dest []struct {
		model.Actor

		Films []struct {
			model.Film

			Language   model.Language
			Categories []model.Category
		}
	}

	err := stmt.QueryContext(ctx, db, &dest)
	require.NoError(t, err)
	require.Len(t, dest, 200)

	if sourceIsCockroachDB() {
		return // char[n] columns whitespaces are trimmed when returned as json in cockroachdb
	}

	//testutils.SaveJSONFile(dest, "./testdata/results/postgres/quick-start-json-dest.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/quick-start-json-dest.json")
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
UPDATE dvds.rental
SET return_date = $1
WHERE rental.rental_id = $2
RETURNING rental.rental_id AS "rental.rental_id",
          rental.rental_date AS "rental.rental_date",
          rental.inventory_id AS "rental.inventory_id",
          rental.customer_id AS "rental.customer_id",
          rental.return_date AS "rental.return_date",
          rental.staff_id AS "rental.staff_id",
          (
               SELECT row_to_json(customer_records) AS "customer_json"
               FROM (
                         SELECT customer.customer_id AS "customerID",
                              customer.store_id AS "storeID",
                              customer.first_name AS "firstName",
                              customer.last_name AS "lastName",
                              customer.email AS "email",
                              customer.address_id AS "addressID",
                              customer.activebool AS "activebool",
                              (to_char(customer.create_date::timestamp, 'YYYY-MM-DD') || 'T00:00:00Z') AS "createDate",
                              to_char(customer.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate",
                              customer.active AS "active"
                         FROM dvds.customer
                         WHERE customer.customer_id = rental.customer_id
                    ) AS customer_records
          ) AS "Customer";
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
	"ReturnDate": "2010-02-04T05:06:07Z",
	"StaffID": 1,
	"LastUpdate": "0001-01-01T00:00:00Z",
	"Customer": {
		"CustomerID": 155,
		"StoreID": 1,
		"FirstName": "Gail",
		"LastName": "Knight",
		"Email": "gail.knight@sakilacustomer.org",
		"AddressID": 159,
		"Activebool": true,
		"CreateDate": "2006-02-14T00:00:00Z",
		"LastUpdate": "2013-05-26T14:49:45.738Z",
		"Active": 1
	}
}
`)
	})
}

func TestSelectJson_FetchFirst(t *testing.T) {
	stmt := SELECT_JSON_ARR(Actor.AllColumns).
		FROM(Actor).
		ORDER_BY(Actor.ActorID).
		OFFSET(2).
		FETCH_FIRST(Int(3)).ROWS_ONLY()

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT json_agg(row_to_json(records)) AS "json"
FROM (
          SELECT actor.actor_id AS "actorID",
               actor.first_name AS "firstName",
               actor.last_name AS "lastName",
               to_char(actor.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
          FROM dvds.actor
          ORDER BY actor.actor_id
          OFFSET 2
          FETCH FIRST 3 ROWS ONLY
     ) AS records;
`)

	var dest []model.Actor

	err := stmt.QueryContext(ctx, db, &dest)
	require.NoError(t, err)
	testutils.AssertJSON(t, dest, `
[
	{
		"ActorID": 3,
		"FirstName": "Ed",
		"LastName": "Chase",
		"LastUpdate": "2013-05-26T14:47:57.62Z"
	},
	{
		"ActorID": 4,
		"FirstName": "Jennifer",
		"LastName": "Davis",
		"LastUpdate": "2013-05-26T14:47:57.62Z"
	},
	{
		"ActorID": 5,
		"FirstName": "Johnny",
		"LastName": "Lollobrigida",
		"LastUpdate": "2013-05-26T14:47:57.62Z"
	}
]
`)
}

func TestSelectJson_RowLock(t *testing.T) {

	stmt := SELECT_JSON_OBJ(Actor.AllColumns).
		FROM(Actor).
		WHERE(Actor.ActorID.EQ(Int(200))).
		FOR(UPDATE().NOWAIT())

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT row_to_json(records) AS "json"
FROM (
          SELECT actor.actor_id AS "actorID",
               actor.first_name AS "firstName",
               actor.last_name AS "lastName",
               to_char(actor.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
          FROM dvds.actor
          WHERE actor.actor_id = 200
          FOR UPDATE NOWAIT
     ) AS records;
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var dest model.Actor

		err := stmt.QueryContext(ctx, tx, &dest)
		require.NoError(t, err)
		testutils.AssertJSON(t, dest, `
{
	"ActorID": 200,
	"FirstName": "Thora",
	"LastName": "Temple",
	"LastUpdate": "2013-05-26T14:47:57.62Z"
}
`)
	})

}

func TestSelectJson_UNION(t *testing.T) {

	stmt := UNION_ALL(
		SELECT_JSON_OBJ(Actor.AllColumns).
			FROM(Actor).
			WHERE(Actor.ActorID.EQ(Int(20))),

		SELECT_JSON_OBJ(Actor.AllColumns).
			FROM(Actor).
			WHERE(Actor.ActorID.EQ(Int(21))),
	)

	testutils.AssertDebugStatementSql(t, stmt, `
(
     SELECT row_to_json(records) AS "json"
     FROM (
               SELECT actor.actor_id AS "actorID",
                    actor.first_name AS "firstName",
                    actor.last_name AS "lastName",
                    to_char(actor.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
               FROM dvds.actor
               WHERE actor.actor_id = 20
          ) AS records
)
UNION ALL
(
     SELECT row_to_json(records) AS "json"
     FROM (
               SELECT actor.actor_id AS "actorID",
                    actor.first_name AS "firstName",
                    actor.last_name AS "lastName",
                    to_char(actor.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
               FROM dvds.actor
               WHERE actor.actor_id = 21
          ) AS records
);
`)

	var dest []struct {
		model.Actor `json_column:"json"`
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	testutils.AssertJSON(t, dest, `
[
	{
		"ActorID": 20,
		"FirstName": "Lucille",
		"LastName": "Tracy",
		"LastUpdate": "2013-05-26T14:47:57.62Z"
	},
	{
		"ActorID": 21,
		"FirstName": "Kirsten",
		"LastName": "Paltrow",
		"LastUpdate": "2013-05-26T14:47:57.62Z"
	}
]
`)
}

func TestSelectJson_Window(t *testing.T) {
	stmt := SELECT_JSON_ARR(
		AVG(Payment.Amount).OVER().AS("avgOver"),
		AVG(Payment.Amount).OVER(Window("w1")).AS("avgOverW1"),
		AVG(Payment.Amount).OVER(
			Window("w2").
				ORDER_BY(Payment.CustomerID).
				RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED)),
		).AS("avgOverW2"),
		AVG(Payment.Amount).OVER(Window("w3").RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED))).AS("avgOverW3"),
	).FROM(
		Payment,
	).WINDOW("w1").AS(PARTITION_BY(Payment.PaymentDate)).
		WINDOW("w2").AS(Window("w1")).
		WINDOW("w3").AS(Window("w2").ORDER_BY(Payment.CustomerID)).
		ORDER_BY(Payment.CustomerID).
		LIMIT(4)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT json_agg(row_to_json(records)) AS "json"
FROM (
          SELECT AVG(payment.amount) OVER () AS "avgOver",
               AVG(payment.amount) OVER (w1) AS "avgOverW1",
               AVG(payment.amount) OVER (w2 ORDER BY payment.customer_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "avgOverW2",
               AVG(payment.amount) OVER (w3 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "avgOverW3"
          FROM dvds.payment
          WINDOW w1 AS (PARTITION BY payment.payment_date), w2 AS (w1), w3 AS (w2 ORDER BY payment.customer_id)
          ORDER BY payment.customer_id
          LIMIT 4
     ) AS records;
`)

	var dest []struct {
		AvgOver   float64
		AvgOverW1 float64
		AvgOverW2 float64
		AvgOverW3 float64
	}

	err := stmt.QueryContext(ctx, db, &dest)
	require.NoError(t, err)
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

	//fmt.Println(stmt.DebugSql())

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT (
          SELECT json_agg(row_to_json(raw_json_records)) AS "raw_json_json"
          FROM (
                    SELECT customer_list.id AS "id",
                         customer_list.name AS "name",
                         customer_list.address AS "address",
                         customer_list."zip code" AS "zip code",
                         customer_list.phone AS "phone",
                         customer_list.city AS "city",
                         customer_list.country AS "country",
                         customer_list.notes AS "notes",
                         customer_list.sid AS "sid",
                         (
                              SELECT json_agg(row_to_json(rentals_records)) AS "rentals_json"
                              FROM (
                                        SELECT rental.rental_id AS "rentalID",
                                             to_char(rental.rental_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "rentalDate",
                                             rental.inventory_id AS "inventoryID",
                                             rental.customer_id AS "customerID",
                                             to_char(rental.return_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "returnDate",
                                             rental.staff_id AS "staffID",
                                             to_char(rental.last_update, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "lastUpdate"
                                        FROM dvds.rental
                                        WHERE customer_list.id = rental.customer_id
                                        ORDER BY rental.customer_id
                                   ) AS rentals_records
                         ) AS "Rentals"
                    FROM dvds.customer_list
                    WHERE customer_list.id <= 2
                    ORDER BY customer_list.id
               ) AS raw_json_records
     ) AS "raw_json";
`)

	var dest struct {
		RawJson []byte
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	if sourceIsCockroachDB() {
		require.Equal(t, string(dest.RawJson), `[{"Rentals": [{"customerID": 1, "inventoryID": 3021, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-05-25T11:30:37.000000Z", "rentalID": 76, "returnDate": "2005-06-03T12:00:37.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 4020, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-05-28T10:35:23.000000Z", "rentalID": 573, "returnDate": "2005-06-03T06:32:23.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 2785, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-06-15T00:54:12.000000Z", "rentalID": 1185, "returnDate": "2005-06-23T02:42:12.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 1021, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-06-15T18:02:53.000000Z", "rentalID": 1422, "returnDate": "2005-06-19T15:54:53.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 1407, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-06-15T21:08:46.000000Z", "rentalID": 1476, "returnDate": "2005-06-25T02:26:46.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 726, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-06-16T15:18:57.000000Z", "rentalID": 1725, "returnDate": "2005-06-17T21:05:57.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 197, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-06-18T08:41:48.000000Z", "rentalID": 2308, "returnDate": "2005-06-22T03:36:48.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 3497, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-06-18T13:33:59.000000Z", "rentalID": 2363, "returnDate": "2005-06-19T17:40:59.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 4566, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-06-21T06:24:45.000000Z", "rentalID": 3284, "returnDate": "2005-06-28T03:28:45.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 1443, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-08T03:17:05.000000Z", "rentalID": 4526, "returnDate": "2005-07-14T01:19:05.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 3486, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-08T07:33:56.000000Z", "rentalID": 4611, "returnDate": "2005-07-12T13:25:56.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 3726, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-09T13:24:07.000000Z", "rentalID": 5244, "returnDate": "2005-07-14T14:01:07.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 797, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-09T16:38:01.000000Z", "rentalID": 5326, "returnDate": "2005-07-13T18:02:01.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 1330, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-11T10:13:46.000000Z", "rentalID": 6163, "returnDate": "2005-07-19T13:15:46.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 2465, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-27T11:31:22.000000Z", "rentalID": 7273, "returnDate": "2005-07-31T06:50:22.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 1092, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-28T09:04:45.000000Z", "rentalID": 7841, "returnDate": "2005-07-30T12:37:45.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 4268, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-28T16:18:23.000000Z", "rentalID": 8033, "returnDate": "2005-07-30T17:56:23.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 1558, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-28T17:33:39.000000Z", "rentalID": 8074, "returnDate": "2005-07-29T20:17:39.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 4497, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-28T19:20:07.000000Z", "rentalID": 8116, "returnDate": "2005-07-29T22:54:07.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 108, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-29T03:58:49.000000Z", "rentalID": 8326, "returnDate": "2005-08-01T05:16:49.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 2219, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-31T02:42:18.000000Z", "rentalID": 9571, "returnDate": "2005-08-02T23:26:18.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 14, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-01T08:51:04.000000Z", "rentalID": 10437, "returnDate": "2005-08-10T12:12:04.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 3232, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-02T15:36:52.000000Z", "rentalID": 11299, "returnDate": "2005-08-10T16:40:52.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 1440, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-02T18:01:38.000000Z", "rentalID": 11367, "returnDate": "2005-08-04T13:19:38.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 2639, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-17T12:37:54.000000Z", "rentalID": 11824, "returnDate": "2005-08-19T10:11:54.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 921, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-18T03:57:29.000000Z", "rentalID": 12250, "returnDate": "2005-08-22T23:05:29.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 3019, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-19T09:55:16.000000Z", "rentalID": 13068, "returnDate": "2005-08-20T14:44:16.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 2269, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-19T13:56:54.000000Z", "rentalID": 13176, "returnDate": "2005-08-23T08:50:54.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 4249, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-21T23:33:57.000000Z", "rentalID": 14762, "returnDate": "2005-08-23T01:30:57.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 1449, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-22T01:27:57.000000Z", "rentalID": 14825, "returnDate": "2005-08-27T07:01:57.000000Z", "staffID": 2}, {"customerID": 1, "inventoryID": 1446, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-22T19:41:37.000000Z", "rentalID": 15298, "returnDate": "2005-08-28T22:49:37.000000Z", "staffID": 1}, {"customerID": 1, "inventoryID": 312, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-22T20:03:46.000000Z", "rentalID": 15315, "returnDate": "2005-08-30T01:51:46.000000Z", "staffID": 2}], "address": "1913 Hanoi Way", "city": "Sasebo", "country": "Japan", "id": 1, "name": "Mary Smith", "notes": "active", "phone": "28303384290", "sid": 1, "zip code": "35200"}, {"Rentals": [{"customerID": 2, "inventoryID": 1090, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-05-27T00:09:24.000000Z", "rentalID": 320, "returnDate": "2005-05-28T04:30:24.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 352, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-06-17T20:54:58.000000Z", "rentalID": 2128, "returnDate": "2005-06-24T00:41:58.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 4116, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-10T06:31:24.000000Z", "rentalID": 5636, "returnDate": "2005-07-13T02:36:24.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 2760, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-10T12:38:56.000000Z", "rentalID": 5755, "returnDate": "2005-07-19T17:02:56.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 741, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-27T14:30:42.000000Z", "rentalID": 7346, "returnDate": "2005-08-02T16:48:42.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 488, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-27T15:23:02.000000Z", "rentalID": 7376, "returnDate": "2005-08-04T10:35:02.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 2053, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-27T18:40:20.000000Z", "rentalID": 7459, "returnDate": "2005-08-02T21:07:20.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 1937, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-29T00:12:59.000000Z", "rentalID": 8230, "returnDate": "2005-08-06T19:52:59.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 626, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-29T12:56:59.000000Z", "rentalID": 8598, "returnDate": "2005-08-01T08:39:59.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 4038, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-29T17:14:29.000000Z", "rentalID": 8705, "returnDate": "2005-08-02T16:01:29.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 2377, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-30T06:06:10.000000Z", "rentalID": 9031, "returnDate": "2005-08-04T10:45:10.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 4030, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-30T13:47:43.000000Z", "rentalID": 9236, "returnDate": "2005-08-08T18:52:43.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 1382, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-30T14:14:11.000000Z", "rentalID": 9248, "returnDate": "2005-08-05T11:19:11.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 4088, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-30T16:21:13.000000Z", "rentalID": 9296, "returnDate": "2005-08-08T11:57:13.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 3084, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-30T22:39:53.000000Z", "rentalID": 9465, "returnDate": "2005-08-06T16:43:53.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 3142, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-07-31T21:58:56.000000Z", "rentalID": 10136, "returnDate": "2005-08-03T19:44:56.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 138, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-01T09:45:26.000000Z", "rentalID": 10466, "returnDate": "2005-08-06T06:28:26.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 3418, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-02T02:10:56.000000Z", "rentalID": 10918, "returnDate": "2005-08-02T21:23:56.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 654, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-02T07:41:41.000000Z", "rentalID": 11087, "returnDate": "2005-08-10T10:37:41.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 1149, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-02T10:43:48.000000Z", "rentalID": 11177, "returnDate": "2005-08-10T10:55:48.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 2060, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-02T13:44:53.000000Z", "rentalID": 11256, "returnDate": "2005-08-04T16:39:53.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 805, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-17T03:52:18.000000Z", "rentalID": 11614, "returnDate": "2005-08-20T07:04:18.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 1521, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-19T06:26:04.000000Z", "rentalID": 12963, "returnDate": "2005-08-23T11:37:04.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 3164, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-21T13:24:32.000000Z", "rentalID": 14475, "returnDate": "2005-08-27T08:59:32.000000Z", "staffID": 2}, {"customerID": 2, "inventoryID": 4570, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-21T22:41:56.000000Z", "rentalID": 14743, "returnDate": "2005-08-29T00:18:56.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 2179, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-22T13:53:04.000000Z", "rentalID": 15145, "returnDate": "2005-08-31T15:51:04.000000Z", "staffID": 1}, {"customerID": 2, "inventoryID": 2898, "lastUpdate": "2006-02-16T02:30:53.000000Z", "rentalDate": "2005-08-23T17:39:35.000000Z", "rentalID": 15907, "returnDate": "2005-08-25T23:23:35.000000Z", "staffID": 1}], "address": "1121 Loja Avenue", "city": "San Bernardino", "country": "United States", "id": 2, "name": "Patricia Johnson", "notes": "active", "phone": "838635286649", "sid": 1, "zip code": "17886"}]`)
	} else {
		require.Equal(t, string(dest.RawJson), `[{"id":1,"name":"Mary Smith","address":"1913 Hanoi Way","zip code":"35200","phone":"28303384290","city":"Sasebo","country":"Japan","notes":"active","sid":1,"Rentals":[{"rentalID":76,"rentalDate":"2005-05-25T11:30:37.000000Z","inventoryID":3021,"customerID":1,"returnDate":"2005-06-03T12:00:37.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":573,"rentalDate":"2005-05-28T10:35:23.000000Z","inventoryID":4020,"customerID":1,"returnDate":"2005-06-03T06:32:23.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":1185,"rentalDate":"2005-06-15T00:54:12.000000Z","inventoryID":2785,"customerID":1,"returnDate":"2005-06-23T02:42:12.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":1422,"rentalDate":"2005-06-15T18:02:53.000000Z","inventoryID":1021,"customerID":1,"returnDate":"2005-06-19T15:54:53.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":1476,"rentalDate":"2005-06-15T21:08:46.000000Z","inventoryID":1407,"customerID":1,"returnDate":"2005-06-25T02:26:46.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":1725,"rentalDate":"2005-06-16T15:18:57.000000Z","inventoryID":726,"customerID":1,"returnDate":"2005-06-17T21:05:57.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":2308,"rentalDate":"2005-06-18T08:41:48.000000Z","inventoryID":197,"customerID":1,"returnDate":"2005-06-22T03:36:48.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":2363,"rentalDate":"2005-06-18T13:33:59.000000Z","inventoryID":3497,"customerID":1,"returnDate":"2005-06-19T17:40:59.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":3284,"rentalDate":"2005-06-21T06:24:45.000000Z","inventoryID":4566,"customerID":1,"returnDate":"2005-06-28T03:28:45.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":4526,"rentalDate":"2005-07-08T03:17:05.000000Z","inventoryID":1443,"customerID":1,"returnDate":"2005-07-14T01:19:05.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":4611,"rentalDate":"2005-07-08T07:33:56.000000Z","inventoryID":3486,"customerID":1,"returnDate":"2005-07-12T13:25:56.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":5244,"rentalDate":"2005-07-09T13:24:07.000000Z","inventoryID":3726,"customerID":1,"returnDate":"2005-07-14T14:01:07.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":5326,"rentalDate":"2005-07-09T16:38:01.000000Z","inventoryID":797,"customerID":1,"returnDate":"2005-07-13T18:02:01.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":6163,"rentalDate":"2005-07-11T10:13:46.000000Z","inventoryID":1330,"customerID":1,"returnDate":"2005-07-19T13:15:46.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":7273,"rentalDate":"2005-07-27T11:31:22.000000Z","inventoryID":2465,"customerID":1,"returnDate":"2005-07-31T06:50:22.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":7841,"rentalDate":"2005-07-28T09:04:45.000000Z","inventoryID":1092,"customerID":1,"returnDate":"2005-07-30T12:37:45.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":8033,"rentalDate":"2005-07-28T16:18:23.000000Z","inventoryID":4268,"customerID":1,"returnDate":"2005-07-30T17:56:23.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":8074,"rentalDate":"2005-07-28T17:33:39.000000Z","inventoryID":1558,"customerID":1,"returnDate":"2005-07-29T20:17:39.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":8116,"rentalDate":"2005-07-28T19:20:07.000000Z","inventoryID":4497,"customerID":1,"returnDate":"2005-07-29T22:54:07.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":8326,"rentalDate":"2005-07-29T03:58:49.000000Z","inventoryID":108,"customerID":1,"returnDate":"2005-08-01T05:16:49.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":9571,"rentalDate":"2005-07-31T02:42:18.000000Z","inventoryID":2219,"customerID":1,"returnDate":"2005-08-02T23:26:18.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":10437,"rentalDate":"2005-08-01T08:51:04.000000Z","inventoryID":14,"customerID":1,"returnDate":"2005-08-10T12:12:04.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":11299,"rentalDate":"2005-08-02T15:36:52.000000Z","inventoryID":3232,"customerID":1,"returnDate":"2005-08-10T16:40:52.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":11367,"rentalDate":"2005-08-02T18:01:38.000000Z","inventoryID":1440,"customerID":1,"returnDate":"2005-08-04T13:19:38.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":11824,"rentalDate":"2005-08-17T12:37:54.000000Z","inventoryID":2639,"customerID":1,"returnDate":"2005-08-19T10:11:54.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":12250,"rentalDate":"2005-08-18T03:57:29.000000Z","inventoryID":921,"customerID":1,"returnDate":"2005-08-22T23:05:29.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":13068,"rentalDate":"2005-08-19T09:55:16.000000Z","inventoryID":3019,"customerID":1,"returnDate":"2005-08-20T14:44:16.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":13176,"rentalDate":"2005-08-19T13:56:54.000000Z","inventoryID":2269,"customerID":1,"returnDate":"2005-08-23T08:50:54.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":14762,"rentalDate":"2005-08-21T23:33:57.000000Z","inventoryID":4249,"customerID":1,"returnDate":"2005-08-23T01:30:57.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":14825,"rentalDate":"2005-08-22T01:27:57.000000Z","inventoryID":1449,"customerID":1,"returnDate":"2005-08-27T07:01:57.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":15298,"rentalDate":"2005-08-22T19:41:37.000000Z","inventoryID":1446,"customerID":1,"returnDate":"2005-08-28T22:49:37.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":15315,"rentalDate":"2005-08-22T20:03:46.000000Z","inventoryID":312,"customerID":1,"returnDate":"2005-08-30T01:51:46.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}]}, {"id":2,"name":"Patricia Johnson","address":"1121 Loja Avenue","zip code":"17886","phone":"838635286649","city":"San Bernardino","country":"United States","notes":"active","sid":1,"Rentals":[{"rentalID":320,"rentalDate":"2005-05-27T00:09:24.000000Z","inventoryID":1090,"customerID":2,"returnDate":"2005-05-28T04:30:24.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":2128,"rentalDate":"2005-06-17T20:54:58.000000Z","inventoryID":352,"customerID":2,"returnDate":"2005-06-24T00:41:58.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":5636,"rentalDate":"2005-07-10T06:31:24.000000Z","inventoryID":4116,"customerID":2,"returnDate":"2005-07-13T02:36:24.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":5755,"rentalDate":"2005-07-10T12:38:56.000000Z","inventoryID":2760,"customerID":2,"returnDate":"2005-07-19T17:02:56.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":7346,"rentalDate":"2005-07-27T14:30:42.000000Z","inventoryID":741,"customerID":2,"returnDate":"2005-08-02T16:48:42.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":7376,"rentalDate":"2005-07-27T15:23:02.000000Z","inventoryID":488,"customerID":2,"returnDate":"2005-08-04T10:35:02.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":7459,"rentalDate":"2005-07-27T18:40:20.000000Z","inventoryID":2053,"customerID":2,"returnDate":"2005-08-02T21:07:20.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":8230,"rentalDate":"2005-07-29T00:12:59.000000Z","inventoryID":1937,"customerID":2,"returnDate":"2005-08-06T19:52:59.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":8598,"rentalDate":"2005-07-29T12:56:59.000000Z","inventoryID":626,"customerID":2,"returnDate":"2005-08-01T08:39:59.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":8705,"rentalDate":"2005-07-29T17:14:29.000000Z","inventoryID":4038,"customerID":2,"returnDate":"2005-08-02T16:01:29.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":9031,"rentalDate":"2005-07-30T06:06:10.000000Z","inventoryID":2377,"customerID":2,"returnDate":"2005-08-04T10:45:10.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":9236,"rentalDate":"2005-07-30T13:47:43.000000Z","inventoryID":4030,"customerID":2,"returnDate":"2005-08-08T18:52:43.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":9248,"rentalDate":"2005-07-30T14:14:11.000000Z","inventoryID":1382,"customerID":2,"returnDate":"2005-08-05T11:19:11.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":9296,"rentalDate":"2005-07-30T16:21:13.000000Z","inventoryID":4088,"customerID":2,"returnDate":"2005-08-08T11:57:13.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":9465,"rentalDate":"2005-07-30T22:39:53.000000Z","inventoryID":3084,"customerID":2,"returnDate":"2005-08-06T16:43:53.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":10136,"rentalDate":"2005-07-31T21:58:56.000000Z","inventoryID":3142,"customerID":2,"returnDate":"2005-08-03T19:44:56.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":10466,"rentalDate":"2005-08-01T09:45:26.000000Z","inventoryID":138,"customerID":2,"returnDate":"2005-08-06T06:28:26.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":10918,"rentalDate":"2005-08-02T02:10:56.000000Z","inventoryID":3418,"customerID":2,"returnDate":"2005-08-02T21:23:56.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":11087,"rentalDate":"2005-08-02T07:41:41.000000Z","inventoryID":654,"customerID":2,"returnDate":"2005-08-10T10:37:41.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":11177,"rentalDate":"2005-08-02T10:43:48.000000Z","inventoryID":1149,"customerID":2,"returnDate":"2005-08-10T10:55:48.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":11256,"rentalDate":"2005-08-02T13:44:53.000000Z","inventoryID":2060,"customerID":2,"returnDate":"2005-08-04T16:39:53.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":11614,"rentalDate":"2005-08-17T03:52:18.000000Z","inventoryID":805,"customerID":2,"returnDate":"2005-08-20T07:04:18.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":12963,"rentalDate":"2005-08-19T06:26:04.000000Z","inventoryID":1521,"customerID":2,"returnDate":"2005-08-23T11:37:04.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":14475,"rentalDate":"2005-08-21T13:24:32.000000Z","inventoryID":3164,"customerID":2,"returnDate":"2005-08-27T08:59:32.000000Z","staffID":2,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":14743,"rentalDate":"2005-08-21T22:41:56.000000Z","inventoryID":4570,"customerID":2,"returnDate":"2005-08-29T00:18:56.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":15145,"rentalDate":"2005-08-22T13:53:04.000000Z","inventoryID":2179,"customerID":2,"returnDate":"2005-08-31T15:51:04.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}, {"rentalID":15907,"rentalDate":"2005-08-23T17:39:35.000000Z","inventoryID":2898,"customerID":2,"returnDate":"2005-08-25T23:23:35.000000Z","staffID":1,"lastUpdate":"2006-02-16T02:30:53.000000Z"}]}]`)
	}
}

func TestSelectJsonObject_EmptyResult(t *testing.T) {
	t.Run("json obj", func(t *testing.T) {
		stmt := SELECT_JSON_OBJ(Actor.AllColumns).
			FROM(Actor).
			WHERE(Actor.FirstName.EQ(Text("Kowalski")))

		var dest model.Actor

		err := stmt.QueryContext(ctx, db, &dest)
		require.ErrorIs(t, err, qrm.ErrNoRows)
	})

	t.Run("json arr", func(t *testing.T) {
		stmt := SELECT_JSON_ARR(Actor.AllColumns).
			FROM(Actor).
			WHERE(Actor.FirstName.EQ(Text("Kowalski")))

		var dest []model.Actor

		err := stmt.QueryContext(ctx, db, &dest)
		require.NoError(t, err)
		require.Empty(t, dest)
	})
}

func TestSelectJson_InvalidDestination(t *testing.T) {
	t.Run("json obj", func(t *testing.T) {
		stmt := SELECT_JSON_OBJ(Actor.AllColumns).
			FROM(Actor)

		testutils.AssertQueryPanicErr(t, stmt, db, &[]model.Actor{}, "jet: SELECT_JSON_OBJ destination has to be a pointer to struct or pointer to map[string]any")
		testutils.AssertQueryPanicErr(t, stmt, db, model.Actor{}, "jet: SELECT_JSON_OBJ destination has to be a pointer to struct or pointer to map[string]any")
		testutils.AssertQueryPanicErr(t, stmt, nil, &model.Actor{}, "jet: db is nil")
		testutils.AssertQueryPanicErr(t, stmt, db, nil, "jet: destination is nil")
	})

	t.Run("json arr", func(t *testing.T) {
		stmt := SELECT_JSON_ARR(Actor.AllColumns).
			FROM(Actor)

		testutils.AssertQueryPanicErr(t, stmt, db, &model.Actor{}, "jet: SELECT_JSON_ARR destination has to be a pointer to slice of struct or pointer to []map[string]any")
		testutils.AssertQueryPanicErr(t, stmt, db, []model.Actor{}, "jet: SELECT_JSON_ARR destination has to be a pointer to slice of struct or pointer to []map[string]any")
		testutils.AssertQueryPanicErr(t, stmt, nil, &[]model.Actor{}, "jet: db is nil")
		testutils.AssertQueryPanicErr(t, stmt, db, nil, "jet: destination is nil")
	})
}

func TestSelectJson_ProjectionNotAliased(t *testing.T) {
	t.Run("statement not aliased", func(t *testing.T) {
		testutils.AssertPanicErr(t, func() {
			stmt := SELECT_JSON_ARR(
				Customer.AllColumns,

				SELECT_JSON_ARR(Rental.AllColumns).
					FROM(Rental).
					WHERE(Rental.CustomerID.EQ(Customer.CustomerID)),
			).FROM(Customer)

			stmt.DebugSql()

		}, "jet: SELECT JSON statements need to be aliased when used as a projection.")
	})

	t.Run("expression not aliased", func(t *testing.T) {
		testutils.AssertPanicErr(t, func() {
			stmt := SELECT_JSON_ARR(
				Int(2).ADD(Customer.CustomerID),
			).FROM(Customer)

			stmt.DebugSql()

		}, "jet: expression need to be aliased when used as SELECT JSON projection.")
	})
}

func TestSelectJson_InvalidJson(t *testing.T) {
	stmt := SELECT(
		Bytea("}invalid json {()").AS("invalid_json"),
	)

	var dest struct {
		InvalidJson []byte `json_column:"invalid_json"`
	}
	err := stmt.QueryContext(ctx, db, &dest)
	require.ErrorContains(t, err, "invalid json")
}
