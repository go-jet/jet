package sqlite

import (
	"context"
	model2 "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/chinook/model"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/chinook/table"
	"strings"
	"testing"
	"time"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/view"

	"github.com/stretchr/testify/require"
)

func TestSelect_ScanToStruct(t *testing.T) {
	query := Actor.
		SELECT(Actor.AllColumns).
		DISTINCT().
		WHERE(Actor.ActorID.EQ(Int(2)))

	testutils.AssertStatementSql(t, query, `
SELECT DISTINCT actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update"
FROM actor
WHERE actor.actor_id = ?;
`, int64(2))

	actor := model.Actor{}
	err := query.Query(db, &actor)

	require.NoError(t, err)

	testutils.AssertDeepEqual(t, actor, actor2)
	requireLogged(t, query)
	requireQueryLogged(t, query, 1)
}

var actor2 = model.Actor{
	ActorID:    2,
	FirstName:  "NICK",
	LastName:   "WAHLBERG",
	LastUpdate: *testutils.TimestampWithoutTimeZone("2019-04-11 18:11:48", 2),
}

func TestSelect_ScanToSlice(t *testing.T) {
	query := SELECT(Actor.AllColumns).
		FROM(Actor).
		ORDER_BY(Actor.ActorID)

	testutils.AssertStatementSql(t, query, `
SELECT actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update"
FROM actor
ORDER BY actor.actor_id;
`)
	dest := []model.Actor{}

	err := query.QueryContext(context.Background(), db, &dest)

	require.NoError(t, err)

	require.Equal(t, len(dest), 200)
	testutils.AssertDeepEqual(t, dest[1], actor2)

	//testutils.SaveJSONFile(dest, "./testdata/results/sqlite/all_actors.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/sqlite/all_actors.json")
	requireLogged(t, query)
	requireQueryLogged(t, query, 200)
}

func TestSelectGroupByHaving(t *testing.T) {
	expectedSQL := `
SELECT customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.active AS "customer.active",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     SUM(payment.amount) AS "amount.sum",
     AVG(payment.amount) AS "amount.avg",
     MAX(payment.payment_date) AS "amount.max_date",
     MAX(payment.amount) AS "amount.max",
     MIN(payment.payment_date) AS "amount.min_date",
     MIN(payment.amount) AS "amount.min",
     COUNT(payment.amount) AS "amount.count"
FROM payment
     INNER JOIN customer ON (customer.customer_id = payment.customer_id)
GROUP BY payment.customer_id
HAVING SUM(payment.amount) > 125.6
ORDER BY payment.customer_id, SUM(payment.amount) ASC;
`
	query := Payment.
		INNER_JOIN(Customer, Customer.CustomerID.EQ(Payment.CustomerID)).
		SELECT(
			Customer.AllColumns,

			SUMf(Payment.Amount).AS("amount.sum"),
			AVG(Payment.Amount).AS("amount.avg"),
			MAX(Payment.PaymentDate).AS("amount.max_date"),
			MAXf(Payment.Amount).AS("amount.max"),
			MIN(Payment.PaymentDate).AS("amount.min_date"),
			MINf(Payment.Amount).AS("amount.min"),
			COUNT(Payment.Amount).AS("amount.count"),
		).
		GROUP_BY(Payment.CustomerID).
		HAVING(
			SUMf(Payment.Amount).GT(Float(125.6)),
		).
		ORDER_BY(
			Payment.CustomerID, SUMf(Payment.Amount).ASC(),
		)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, float64(125.6))

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

	err := query.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 174)
	//testutils.SaveJSONFile(dest, "./testdata/results/sqlite/customer_payment_sum.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/sqlite/customer_payment_sum.json")
	requireLogged(t, query)
}

func TestOrderBy(t *testing.T) {

	t.Run("default", func(t *testing.T) {
		stmt := SELECT(
			Rental.AllColumns,
		).FROM(
			Rental,
		).ORDER_BY(
			Rental.ReturnDate,
		).LIMIT(200)

		testutils.AssertDebugStatementSql(t, stmt, `
SELECT rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM rental
ORDER BY rental.return_date
LIMIT 200;
`)
		require.NoError(t, stmt.Query(db, &struct{}{}))
	})

	t.Run("NULLS FIRST", func(t *testing.T) {
		stmt := SELECT(
			Rental.AllColumns,
		).FROM(
			Rental,
		).ORDER_BY(
			Rental.ReturnDate.NULLS_FIRST(),
		).LIMIT(200)

		testutils.AssertDebugStatementSql(t, stmt, `
SELECT rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM rental
ORDER BY rental.return_date NULLS FIRST
LIMIT 200;
`)
		require.NoError(t, stmt.Query(db, &struct{}{}))
	})

	t.Run("NULLS LAST", func(t *testing.T) {
		stmt := SELECT(
			Rental.AllColumns,
		).FROM(
			Rental,
		).ORDER_BY(
			Rental.ReturnDate.NULLS_LAST(),
		).LIMIT(200)

		testutils.AssertDebugStatementSql(t, stmt, `
SELECT rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM rental
ORDER BY rental.return_date NULLS LAST
LIMIT 200;
`)
		require.NoError(t, stmt.Query(db, &struct{}{}))
	})

	t.Run("ASC", func(t *testing.T) {
		stmt := SELECT(
			Rental.AllColumns,
		).FROM(
			Rental,
		).ORDER_BY(
			Rental.ReturnDate.ASC(),
		).LIMIT(200)

		testutils.AssertDebugStatementSql(t, stmt, `
SELECT rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM rental
ORDER BY rental.return_date ASC
LIMIT 200;
`)
		require.NoError(t, stmt.Query(db, &struct{}{}))
	})

	t.Run("ASC NULLS FIRST", func(t *testing.T) {
		stmt := SELECT(
			Rental.AllColumns,
		).FROM(
			Rental,
		).ORDER_BY(
			Rental.ReturnDate.ASC().NULLS_FIRST(),
		).LIMIT(200)

		testutils.AssertDebugStatementSql(t, stmt, `
SELECT rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM rental
ORDER BY rental.return_date ASC NULLS FIRST
LIMIT 200;
`)

		require.NoError(t, stmt.Query(db, &struct{}{}))
	})

	t.Run("ASC NULLS LAST", func(t *testing.T) {
		stmt := SELECT(
			Rental.AllColumns,
		).FROM(
			Rental,
		).ORDER_BY(
			Rental.ReturnDate.ASC().NULLS_LAST(),
		).LIMIT(200).OFFSET(15800)

		testutils.AssertDebugStatementSql(t, stmt, `
SELECT rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM rental
ORDER BY rental.return_date ASC NULLS LAST
LIMIT 200
OFFSET 15800;
`)

		require.NoError(t, stmt.Query(db, &struct{}{}))
	})

	t.Run("DESC", func(t *testing.T) {
		stmt := SELECT(
			Rental.AllColumns,
		).FROM(
			Rental,
		).ORDER_BY(
			Rental.ReturnDate.DESC(),
		).LIMIT(200).OFFSET(15800)

		testutils.AssertDebugStatementSql(t, stmt, `
SELECT rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM rental
ORDER BY rental.return_date DESC
LIMIT 200
OFFSET 15800;
`)

		require.NoError(t, stmt.Query(db, &struct{}{}))
	})

	t.Run("DESC NULLS LAST", func(t *testing.T) {
		stmt := SELECT(
			Rental.AllColumns,
		).FROM(
			Rental,
		).ORDER_BY(
			Rental.ReturnDate.DESC().NULLS_LAST(),
		).LIMIT(200).OFFSET(15800)

		testutils.AssertDebugStatementSql(t, stmt, `
SELECT rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM rental
ORDER BY rental.return_date DESC NULLS LAST
LIMIT 200
OFFSET 15800;
`)

		require.NoError(t, stmt.Query(db, &struct{}{}))
	})

	t.Run("DESC NULLS FIRST", func(t *testing.T) {
		stmt := SELECT(
			Rental.AllColumns,
		).FROM(
			Rental,
		).ORDER_BY(
			Rental.ReturnDate.DESC().NULLS_FIRST(),
		).LIMIT(200)

		testutils.AssertDebugStatementSql(t, stmt, `
SELECT rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM rental
ORDER BY rental.return_date DESC NULLS FIRST
LIMIT 200;
`)

		require.NoError(t, stmt.Query(db, &struct{}{}))
	})
}

func TestAggregateFunctionDistinct(t *testing.T) {
	stmt := SELECT(
		Payment.CustomerID,

		COUNT(DISTINCT(Payment.Amount)).AS("distinct.count"),
		SUM(DISTINCT(Payment.Amount)).AS("distinct.sum"),
		AVG(DISTINCT(Payment.Amount)).AS("distinct.avg"),
		MIN(DISTINCT(Payment.PaymentDate)).AS("distinct.first_payment_date"),
		MAX(DISTINCT(Payment.PaymentDate)).AS("distinct.last_payment_date"),
	).FROM(
		Payment,
	).WHERE(
		Payment.CustomerID.EQ(Int(1)),
	).GROUP_BY(
		Payment.CustomerID,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT payment.customer_id AS "payment.customer_id",
     COUNT(DISTINCT payment.amount) AS "distinct.count",
     SUM(DISTINCT payment.amount) AS "distinct.sum",
     AVG(DISTINCT payment.amount) AS "distinct.avg",
     MIN(DISTINCT payment.payment_date) AS "distinct.first_payment_date",
     MAX(DISTINCT payment.payment_date) AS "distinct.last_payment_date"
FROM payment
WHERE payment.customer_id = 1
GROUP BY payment.customer_id;
`)

	type Distinct struct {
		model.Payment

		Count            int64
		Sum              float64
		Avg              float64
		FirstPaymentDate time.Time
		LastPaymentDate  time.Time
	}

	var dest Distinct

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	testutils.AssertJSON(t, dest, `
{
	"PaymentID": 0,
	"CustomerID": 1,
	"StaffID": 0,
	"RentalID": null,
	"Amount": 0,
	"PaymentDate": "0001-01-01T00:00:00Z",
	"LastUpdate": "0001-01-01T00:00:00Z",
	"Count": 8,
	"Sum": 38.92000000000001,
	"Avg": 4.865000000000001,
	"FirstPaymentDate": "2005-05-25T11:30:37Z",
	"LastPaymentDate": "2005-08-22T20:03:46Z"
}
`)
}

func TestSubQuery(t *testing.T) {

	rRatingFilms :=
		SELECT(
			Film.FilmID,
			Film.Title,
			Film.Rating,
		).FROM(
			Film,
		).WHERE(Film.Rating.EQ(String("R"))).
			AsTable("rFilms")

	rFilmID := Film.FilmID.From(rRatingFilms)

	main :=
		SELECT(
			Actor.AllColumns,
			FilmActor.AllColumns,
			rRatingFilms.AllColumns(),
		).FROM(
			rRatingFilms.
				INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(rFilmID)).
				INNER_JOIN(Actor, Actor.ActorID.EQ(FilmActor.ActorID)),
		).ORDER_BY(
			rFilmID,
			Actor.ActorID,
		)

	var dest []struct {
		model.Film
		Actors []model.Actor
	}

	err := main.Query(db, &dest)
	require.NoError(t, err)

	//testutils.SaveJSONFile(dest, "./testdata/results/sqlite/r_rating_films.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/sqlite/r_rating_films.json")
}

func TestSelectAndUnionInProjection(t *testing.T) {
	query := UNION(
		SELECT(
			Payment.PaymentID,
		).FROM(Payment),

		SELECT(
			STAR,
		).FROM(
			SELECT(Payment.PaymentID).
				FROM(Payment).LIMIT(1).OFFSET(2).AsTable("p"),
		),
	).LIMIT(1).OFFSET(10)

	testutils.AssertDebugStatementSql(t, query, `

SELECT payment.payment_id AS "payment.payment_id"
FROM payment

UNION

SELECT *
FROM (
          SELECT payment.payment_id AS "payment.payment_id"
          FROM payment
          LIMIT 1
          OFFSET 2
     ) AS p
LIMIT 1
OFFSET 10;
`, int64(1), int64(2), int64(1), int64(10))

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestSelectUNION(t *testing.T) {
	expectedSQL := `

SELECT payment.payment_id AS "payment.payment_id"
FROM payment
WHERE payment.payment_id > ?

UNION

SELECT payment.payment_id AS "payment.payment_id"
FROM payment
WHERE payment.amount < ?
LIMIT ?;
`
	query := UNION(
		SELECT(Payment.PaymentID).
			FROM(Payment).
			WHERE(Payment.PaymentID.GT(Int(11))),

		SELECT(Payment.PaymentID).
			FROM(Payment).
			WHERE(Payment.Amount.LT(Float(2000.0))),
	).LIMIT(1)

	testutils.AssertStatementSql(t, query, expectedSQL, int64(11), 2000.0, int64(1))

	query2 :=
		SELECT(
			Payment.PaymentID,
		).FROM(
			Payment,
		).WHERE(
			Payment.PaymentID.GT(Int(11)),
		).UNION(
			SELECT(Payment.PaymentID).
				FROM(Payment).
				WHERE(Payment.Amount.LT(Float(2000.0))),
		).LIMIT(1)

	testutils.AssertStatementSql(t, query2, expectedSQL, int64(11), 2000.0, int64(1))

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestSelectUNION_ALL(t *testing.T) {
	expectedSQL := `

SELECT payment.payment_id AS "payment.payment_id"
FROM payment
WHERE payment.payment_id > ?

UNION ALL

SELECT payment.payment_id AS "payment.payment_id"
FROM payment
WHERE payment.amount < ?
LIMIT ?;
`
	query := UNION_ALL(
		SELECT(Payment.PaymentID).
			FROM(Payment).
			WHERE(Payment.PaymentID.GT(Int(11))),

		SELECT(Payment.PaymentID).
			FROM(Payment).
			WHERE(Payment.Amount.LT(Float(2000.0))),
	).LIMIT(1)

	testutils.AssertStatementSql(t, query, expectedSQL, int64(11), 2000.0, int64(1))

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestJoinQueryStruct(t *testing.T) {

	expectedSQL := `
SELECT film_actor.actor_id AS "film_actor.actor_id",
     film_actor.film_id AS "film_actor.film_id",
     film_actor.last_update AS "film_actor.last_update",
     film.film_id AS "film.film_id",
     film.title AS "film.title",
     film.description AS "film.description",
     film.release_year AS "film.release_year",
     film.language_id AS "film.language_id",
     film.original_language_id AS "film.original_language_id",
     film.rental_duration AS "film.rental_duration",
     film.rental_rate AS "film.rental_rate",
     film.length AS "film.length",
     film.replacement_cost AS "film.replacement_cost",
     film.rating AS "film.rating",
     film.special_features AS "film.special_features",
     film.last_update AS "film.last_update",
     language.language_id AS "language.language_id",
     language.name AS "language.name",
     language.last_update AS "language.last_update",
     actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update",
     inventory.inventory_id AS "inventory.inventory_id",
     inventory.film_id AS "inventory.film_id",
     inventory.store_id AS "inventory.store_id",
     inventory.last_update AS "inventory.last_update",
     rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM language
     INNER JOIN film ON (film.language_id = language.language_id)
     INNER JOIN film_actor ON (film_actor.film_id = film.film_id)
     INNER JOIN actor ON (actor.actor_id = film_actor.actor_id)
     LEFT JOIN inventory ON (inventory.film_id = film.film_id)
     LEFT JOIN rental ON (rental.inventory_id = inventory.inventory_id)
ORDER BY language.language_id ASC, film.film_id ASC, actor.actor_id ASC, inventory.inventory_id ASC, rental.rental_id ASC
LIMIT ?;
`
	for i := 0; i < 2; i++ {
		query :=
			SELECT(
				FilmActor.AllColumns,
				Film.AllColumns,
				Language.AllColumns,
				Actor.AllColumns,
				Inventory.AllColumns,
				Rental.AllColumns,
			).
				FROM(
					Language.
						INNER_JOIN(Film, Film.LanguageID.EQ(Language.LanguageID)).
						INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(Film.FilmID)).
						INNER_JOIN(Actor, Actor.ActorID.EQ(FilmActor.ActorID)).
						LEFT_JOIN(Inventory, Inventory.FilmID.EQ(Film.FilmID)).
						LEFT_JOIN(Rental, Rental.InventoryID.EQ(Inventory.InventoryID)),
				).ORDER_BY(
				Language.LanguageID.ASC(),
				Film.FilmID.ASC(),
				Actor.ActorID.ASC(),
				Inventory.InventoryID.ASC(),
				Rental.RentalID.ASC(),
			).
				LIMIT(1000)

		testutils.AssertStatementSql(t, query, expectedSQL, int64(1000))

		var dest []struct {
			model.Language

			Films []struct {
				model.Film

				Actors []struct {
					model.Actor
				}

				Inventories *[]struct {
					model.Inventory

					Rentals *[]model.Rental
				}
			}
		}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertJSONFile(t, dest, "./testdata/results/sqlite/lang_film_actor_inventory_rental.json")
	}
}

func TestExpressionWrappers(t *testing.T) {
	query := SELECT(
		BoolExp(Raw("true")),
		IntExp(Raw("11")),
		FloatExp(Raw("11.22")),
		StringExp(Raw("'stringer'")),
		TimeExp(Raw("'raw'")),
		TimestampExp(Raw("'raw'")),
		DateTimeExp(Raw("'raw'")),
		DateExp(Raw("'date'")),
	)

	testutils.AssertStatementSql(t, query, `
SELECT true,
     11,
     11.22,
     'stringer',
     'raw',
     'raw',
     'raw',
     'date';
`)

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestWindowFunction(t *testing.T) {
	var expectedSQL = `
SELECT AVG(payment.amount) OVER (),
     AVG(payment.amount) OVER (PARTITION BY payment.customer_id),
     MAX(payment.amount) OVER (ORDER BY payment.payment_date DESC),
     MIN(payment.amount) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC),
     SUM(payment.amount) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC ROWS BETWEEN 1 PRECEDING AND 6 FOLLOWING),
     SUM(payment.amount) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
     MAX(payment.customer_id) OVER (ORDER BY payment.payment_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
     MIN(payment.customer_id) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC),
     SUM(payment.customer_id) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC),
     ROW_NUMBER() OVER (ORDER BY payment.payment_date),
     RANK() OVER (ORDER BY payment.payment_date),
     DENSE_RANK() OVER (ORDER BY payment.payment_date),
     CUME_DIST() OVER (ORDER BY payment.payment_date),
     NTILE(11) OVER (ORDER BY payment.payment_date),
     LAG(payment.amount) OVER (ORDER BY payment.payment_date),
     LAG(payment.amount) OVER (ORDER BY payment.payment_date),
     LAG(payment.amount, 2, payment.amount) OVER (ORDER BY payment.payment_date),
     LAG(payment.amount, 2, ?) OVER (ORDER BY payment.payment_date),
     LEAD(payment.amount) OVER (ORDER BY payment.payment_date),
     LEAD(payment.amount) OVER (ORDER BY payment.payment_date),
     LEAD(payment.amount, 2, payment.amount) OVER (ORDER BY payment.payment_date),
     LEAD(payment.amount, 2, ?) OVER (ORDER BY payment.payment_date),
     FIRST_VALUE(payment.amount) OVER (ORDER BY payment.payment_date),
     LAST_VALUE(payment.amount) OVER (ORDER BY payment.payment_date),
     NTH_VALUE(payment.amount, 3) OVER (ORDER BY payment.payment_date)
FROM payment
WHERE payment.payment_id < ?
GROUP BY payment.amount, payment.customer_id, payment.payment_date;
`
	query :=
		SELECT(
			AVG(Payment.Amount).OVER(),
			AVG(Payment.Amount).OVER(PARTITION_BY(Payment.CustomerID)),
			MAXf(Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate.DESC())),
			MINf(Payment.Amount).OVER(PARTITION_BY(Payment.CustomerID).ORDER_BY(Payment.PaymentDate.DESC())),
			SUMf(Payment.Amount).OVER(PARTITION_BY(Payment.CustomerID).
				ORDER_BY(Payment.PaymentDate.DESC()).ROWS(PRECEDING(1), FOLLOWING(6))),
			SUMf(Payment.Amount).OVER(PARTITION_BY(Payment.CustomerID).
				ORDER_BY(Payment.PaymentDate.DESC()).RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED))),
			MAXi(Payment.CustomerID).OVER(ORDER_BY(Payment.PaymentDate.DESC()).ROWS(CURRENT_ROW, FOLLOWING(UNBOUNDED))),
			MINi(Payment.CustomerID).OVER(PARTITION_BY(Payment.CustomerID).ORDER_BY(Payment.PaymentDate.DESC())),
			SUMi(Payment.CustomerID).OVER(PARTITION_BY(Payment.CustomerID).ORDER_BY(Payment.PaymentDate.DESC())),
			ROW_NUMBER().OVER(ORDER_BY(Payment.PaymentDate)),
			RANK().OVER(ORDER_BY(Payment.PaymentDate)),
			DENSE_RANK().OVER(ORDER_BY(Payment.PaymentDate)),
			CUME_DIST().OVER(ORDER_BY(Payment.PaymentDate)),
			NTILE(11).OVER(ORDER_BY(Payment.PaymentDate)),
			LAG(Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			LAG(Payment.Amount, 2).OVER(ORDER_BY(Payment.PaymentDate)),
			LAG(Payment.Amount, 2, Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			LAG(Payment.Amount, 2, 100).OVER(ORDER_BY(Payment.PaymentDate)),
			LEAD(Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			LEAD(Payment.Amount, 2).OVER(ORDER_BY(Payment.PaymentDate)),
			LEAD(Payment.Amount, 2, Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			LEAD(Payment.Amount, 2, 100).OVER(ORDER_BY(Payment.PaymentDate)),
			FIRST_VALUE(Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			LAST_VALUE(Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			NTH_VALUE(Payment.Amount, 3).OVER(ORDER_BY(Payment.PaymentDate)),
		).FROM(
			Payment,
		).GROUP_BY(
			Payment.Amount,
			Payment.CustomerID,
			Payment.PaymentDate,
		).WHERE(Payment.PaymentID.LT(Int(10)))

	testutils.AssertStatementSql(t, query, expectedSQL, 100, 100, int64(10))

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestWindowClause(t *testing.T) {
	var expectedSQL = `
SELECT AVG(payment.amount) OVER (),
     AVG(payment.amount) OVER (w1),
     AVG(payment.amount) OVER (w2 ORDER BY payment.customer_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
     AVG(payment.amount) OVER (w3 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM payment
WHERE payment.payment_id < ?
WINDOW w1 AS (PARTITION BY payment.payment_date), w2 AS (w1), w3 AS (w2 ORDER BY payment.customer_id)
ORDER BY payment.customer_id;
`
	query := SELECT(
		AVG(Payment.Amount).OVER(),
		AVG(Payment.Amount).OVER(Window("w1")),
		AVG(Payment.Amount).OVER(
			Window("w2").
				ORDER_BY(Payment.CustomerID).
				RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED)),
		),
		AVG(Payment.Amount).OVER(Window("w3").RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED))),
	).FROM(
		Payment,
	).WHERE(
		Payment.PaymentID.LT(Int(10)),
	).
		WINDOW("w1").AS(PARTITION_BY(Payment.PaymentDate)).
		WINDOW("w2").AS(Window("w1")).
		WINDOW("w3").AS(Window("w2").ORDER_BY(Payment.CustomerID)).
		ORDER_BY(
			Payment.CustomerID,
		)

	testutils.AssertStatementSql(t, query, expectedSQL, int64(10))

	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestSimpleView(t *testing.T) {
	query :=
		SELECT(
			view.CustomerList.AllColumns,
		).FROM(
			view.CustomerList,
		).ORDER_BY(
			view.CustomerList.ID,
		).LIMIT(10)

	var dest []model.CustomerList

	err := query.Query(db, &dest)
	require.NoError(t, err)

	require.Equal(t, len(dest), 10)
	require.Equal(t, dest[2], model.CustomerList{
		ID:      testutils.PtrOf(int32(3)),
		Name:    testutils.PtrOf("LINDA WILLIAMS"),
		Address: testutils.PtrOf("692 Joliet Street"),
		ZipCode: testutils.PtrOf("83579"),
		Phone:   testutils.PtrOf(" "),
		City:    testutils.PtrOf("Athenai"),
		Country: testutils.PtrOf("Greece"),
		Notes:   testutils.PtrOf("active"),
		Sid:     testutils.PtrOf(int32(1)),
	})
}

func TestJoinViewWithTable(t *testing.T) {
	query :=
		SELECT(
			view.CustomerList.AllColumns,
			Rental.AllColumns,
		).FROM(
			view.CustomerList.
				INNER_JOIN(Rental, view.CustomerList.ID.EQ(Rental.CustomerID)),
		).ORDER_BY(
			view.CustomerList.ID,
		).WHERE(
			view.CustomerList.ID.LT_EQ(Int(2)),
		)

	var dest []struct {
		model.CustomerList `sql:"primary_key=ID"`
		Rentals            []model.Rental
	}

	err := query.Query(db, &dest)
	require.NoError(t, err)

	require.Equal(t, len(dest), 2)
	require.Equal(t, len(dest[0].Rentals), 32)
	require.Equal(t, len(dest[1].Rentals), 27)
}

func TestConditionalProjectionList(t *testing.T) {
	projectionList := ProjectionList{}

	columnsToSelect := []string{"customer_id", "create_date"}

	for _, columnName := range columnsToSelect {
		switch columnName {
		case Customer.CustomerID.Name():
			projectionList = append(projectionList, Customer.CustomerID)
		case Customer.Email.Name():
			projectionList = append(projectionList, Customer.Email)
		case Customer.CreateDate.Name():
			projectionList = append(projectionList, Customer.CreateDate)
		}
	}

	stmt := SELECT(projectionList).
		FROM(Customer).
		LIMIT(3)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT customer.customer_id AS "customer.customer_id",
     customer.create_date AS "customer.create_date"
FROM customer
LIMIT 3;
`)
	var dest []model.Customer
	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	require.Equal(t, len(dest), 3)
}

func TestUseAttachedDatabase(t *testing.T) {
	Artists := table.Artists.FromSchema("chinook")
	Albums := table.Albums.FromSchema("chinook")

	stmt :=
		SELECT(
			Artists.AllColumns,
			Albums.AllColumns,
		).FROM(
			Albums.
				INNER_JOIN(Artists, Artists.ArtistId.EQ(Albums.ArtistId)),
		).ORDER_BY(
			Artists.ArtistId,
		).LIMIT(10)

	testutils.AssertDebugStatementSql(t, stmt, strings.Replace(`
SELECT artists.''ArtistId'' AS "artists.ArtistId",
     artists.''Name'' AS "artists.Name",
     albums.''AlbumId'' AS "albums.AlbumId",
     albums.''Title'' AS "albums.Title",
     albums.''ArtistId'' AS "albums.ArtistId"
FROM chinook.albums
     INNER JOIN chinook.artists ON (artists.''ArtistId'' = albums.''ArtistId'')
ORDER BY artists.''ArtistId''
LIMIT 10;
`, "''", "`", -1))

	var dest []struct {
		model2.Artists
		Albums []model2.Albums
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	require.Len(t, dest, 7)
}

func TestUseSchema(t *testing.T) {
	table.UseSchema("chinook")
	defer table.UseSchema("")

	stmt := SELECT(
		table.Artists.AllColumns,
	).FROM(
		table.Artists,
	).WHERE(table.Artists.ArtistId.EQ(Int(11)))

	testutils.AssertDebugStatementSql(t, stmt, strings.Replace(`
SELECT artists.''ArtistId'' AS "artists.ArtistId",
     artists.''Name'' AS "artists.Name"
FROM chinook.artists
WHERE artists.''ArtistId'' = 11;
`, "''", "`", -1))

	var artist model2.Artists

	err := stmt.Query(db, &artist)
	require.NoError(t, err)

	testutils.AssertJSON(t, artist, `
{
	"ArtistId": 11,
	"Name": "Black Label Society"
}
`)
}

func TestRowsScan(t *testing.T) {
	stmt :=
		SELECT(
			Inventory.AllColumns,
		).FROM(
			Inventory,
		).ORDER_BY(
			Inventory.InventoryID.ASC(),
		)

	rows, err := stmt.Rows(context.Background(), db)
	require.NoError(t, err)

	for rows.Next() {
		var inventory model.Inventory
		err = rows.Scan(&inventory)
		require.NoError(t, err)

		require.NotEqual(t, inventory.InventoryID, uint32(0))
		require.NotEqual(t, inventory.FilmID, uint16(0))
		require.NotEqual(t, inventory.StoreID, uint16(0))
		require.NotEqual(t, inventory.LastUpdate, time.Time{})

		if inventory.InventoryID == 2103 {
			require.Equal(t, inventory.FilmID, int32(456))
			require.Equal(t, inventory.StoreID, int32(2))
			require.Equal(t, inventory.LastUpdate.Format(time.RFC3339), "2019-04-11T18:11:48Z")
		}
	}

	err = rows.Close()
	require.NoError(t, err)
	err = rows.Err()
	require.NoError(t, err)

	requireLogged(t, stmt)
}

func TestScanNumericToNumber(t *testing.T) {
	type Number struct {
		Int8    int8
		UInt8   uint8
		Int16   int16
		UInt16  uint16
		Int32   int32
		UInt32  uint32
		Int64   int64
		UInt64  uint64
		Float32 float32
		Float64 float64
	}

	numeric := CAST(String("1234567890.111")).AS_REAL()

	stmt := SELECT(
		numeric.AS("number.int8"),
		numeric.AS("number.uint8"),
		numeric.AS("number.int16"),
		numeric.AS("number.uint16"),
		numeric.AS("number.int32"),
		numeric.AS("number.uint32"),
		numeric.AS("number.int64"),
		numeric.AS("number.uint64"),
		numeric.AS("number.float32"),
		numeric.AS("number.float64"),
	)

	var number Number
	err := stmt.Query(db, &number)
	require.NoError(t, err)

	require.Equal(t, number.Int8, int8(-46))     // overflow
	require.Equal(t, number.UInt8, uint8(210))   // overflow
	require.Equal(t, number.Int16, int16(722))   // overflow
	require.Equal(t, number.UInt16, uint16(722)) // overflow
	require.Equal(t, number.Int32, int32(1234567890))
	require.Equal(t, number.UInt32, uint32(1234567890))
	require.Equal(t, number.Int64, int64(1234567890))
	require.Equal(t, number.UInt64, uint64(1234567890))
	require.Equal(t, number.Float32, float32(1.234568e+09))
	require.Equal(t, number.Float64, float64(1.234567890111e+09))
}

func TestConditionalFunctions(t *testing.T) {
	stmt := SELECT(
		EXISTS(
			Film.SELECT(Film.FilmID).WHERE(Film.RentalDuration.GT(Int(5))),
		).AS("exists"),
		CASE(Film.Length.GT(Int(120))).
			WHEN(Bool(true)).THEN(String("long film")).
			ELSE(String("short film")).AS("case"),
		COALESCE(Film.Description, String("none")).AS("coalesce"),
		NULLIF(Film.ReleaseYear, Int(200)).AS("null_if"),
	).FROM(
		Film,
	).WHERE(
		Film.FilmID.LT(Int(5)),
	).ORDER_BY(
		Film.FilmID,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT (EXISTS (
          SELECT film.film_id AS "film.film_id"
          FROM film
          WHERE film.rental_duration > 5
     )) AS "exists",
     (CASE (film.length > 120) WHEN TRUE THEN 'long film' ELSE 'short film' END) AS "case",
     COALESCE(film.description, 'none') AS "coalesce",
     NULLIF(film.release_year, 200) AS "null_if"
FROM film
WHERE film.film_id < 5
ORDER BY film.film_id;
`)

	var res []struct {
		Exists   bool
		Case     string
		Coalesce string
		NullIf   string
	}

	err := stmt.Query(db, &res)
	require.NoError(t, err)

	testutils.AssertJSON(t, res, `
[
	{
		"Exists": true,
		"Case": "short film",
		"Coalesce": "A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies",
		"NullIf": "2006"
	},
	{
		"Exists": true,
		"Case": "short film",
		"Coalesce": "A Astounding Epistle of a Database Administrator And a Explorer who must Find a Car in Ancient China",
		"NullIf": "2006"
	},
	{
		"Exists": true,
		"Case": "short film",
		"Coalesce": "A Astounding Reflection of a Lumberjack And a Car who must Sink a Lumberjack in A Baloon Factory",
		"NullIf": "2006"
	},
	{
		"Exists": true,
		"Case": "short film",
		"Coalesce": "A Fanciful Documentary of a Frisbee And a Lumberjack who must Chase a Monkey in A Shark Tank",
		"NullIf": "2006"
	}
]
`)
}
