package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/enum"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/view"

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
FROM dvds.actor
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
	LastUpdate: *testutils.TimestampWithoutTimeZone("2006-02-15 04:34:33", 2),
}

func TestSelect_ScanToSlice(t *testing.T) {
	query := Actor.
		SELECT(Actor.AllColumns).
		ORDER_BY(Actor.ActorID)

	testutils.AssertStatementSql(t, query, `
SELECT actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update"
FROM dvds.actor
ORDER BY actor.actor_id;
`)
	var dest []model.Actor

	err := query.QueryContext(context.Background(), db, &dest)

	require.NoError(t, err)

	require.Equal(t, len(dest), 200)
	testutils.AssertDeepEqual(t, dest[1], actor2)

	//testutils.PrintJson(dest)
	//testutils.SaveJsonFile(dest, "mysql/testdata/all_actors.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/mysql/all_actors.json")
	requireLogged(t, query)
	requireQueryLogged(t, query, 200)
}

func TestSelectGroupByHaving(t *testing.T) {
	if sourceIsMariaDB() {
		return
	}

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
FROM dvds.payment
     INNER JOIN dvds.customer ON (customer.customer_id = payment.customer_id)
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

	//fmt.Println(query.DebugSql())

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

	//testutils.PrintJson(dest)

	require.Equal(t, len(dest), 174)

	//testutils.SaveJsonFile(dest, "mysql/testdata/customer_payment_sum.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/mysql/customer_payment_sum.json")
	requireLogged(t, query)
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
FROM dvds.payment
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
	"Sum": 38.92,
	"Avg": 4.865,
	"FirstPaymentDate": "2005-05-25T11:30:37Z",
	"LastPaymentDate": "2005-08-22T20:03:46Z"
}
`)

}

func TestGroupByWithRollup(t *testing.T) {
	skipForMariaDB(t)

	stmt := SELECT(
		Inventory.FilmID.AS("film_id"),
		Inventory.StoreID.AS("store_id"),
		GROUPING(Inventory.FilmID).AS("grouping_film_id"),
		GROUPING(Inventory.FilmID, Inventory.StoreID).AS("grouping_film_id_store_id"),
		COUNT(STAR).AS("count"),
	).FROM(
		Inventory,
	).WHERE(
		Inventory.FilmID.IN(Int(2), Int(3)),
	).GROUP_BY(
		WITH_ROLLUP(Inventory.FilmID, Inventory.StoreID),
	).ORDER_BY(
		Inventory.FilmID,
		Inventory.StoreID,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT inventory.film_id AS "film_id",
     inventory.store_id AS "store_id",
     GROUPING(inventory.film_id) AS "grouping_film_id",
     GROUPING(inventory.film_id, inventory.store_id) AS "grouping_film_id_store_id",
     COUNT(*) AS "count"
FROM dvds.inventory
WHERE inventory.film_id IN (2, 3)
GROUP BY inventory.film_id, inventory.store_id WITH ROLLUP
ORDER BY inventory.film_id, inventory.store_id;
`)

	var dest []struct {
		FilmID                int
		StoreID               int
		GroupingFilmID        int
		GroupingFilmIDStoreID int
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	testutils.AssertJSON(t, dest, `
[
	{
		"FilmID": 0,
		"StoreID": 0,
		"GroupingFilmID": 1,
		"GroupingFilmIDStoreID": 3
	},
	{
		"FilmID": 2,
		"StoreID": 0,
		"GroupingFilmID": 0,
		"GroupingFilmIDStoreID": 1
	},
	{
		"FilmID": 2,
		"StoreID": 2,
		"GroupingFilmID": 0,
		"GroupingFilmIDStoreID": 0
	},
	{
		"FilmID": 3,
		"StoreID": 0,
		"GroupingFilmID": 0,
		"GroupingFilmIDStoreID": 1
	},
	{
		"FilmID": 3,
		"StoreID": 2,
		"GroupingFilmID": 0,
		"GroupingFilmIDStoreID": 0
	}
]
`)
}

func TestSubQuery(t *testing.T) {

	rRatingFilms := SELECT(
		Film.FilmID,
		Film.Title,
		Film.Rating,
	).FROM(
		Film,
	).WHERE(
		Film.Rating.EQ(enum.FilmRating.R),
	).AsTable("rFilms")

	rFilmID := Film.FilmID.From(rRatingFilms)

	query := rRatingFilms.
		INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(rFilmID)).
		INNER_JOIN(Actor, Actor.ActorID.EQ(FilmActor.ActorID)).
		SELECT(
			Actor.AllColumns,
			FilmActor.AllColumns,
			rRatingFilms.AllColumns(),
		).
		ORDER_BY(rFilmID, Actor.ActorID)

	var dest []struct {
		model.Film

		Actors []model.Actor
	}

	err := query.Query(db, &dest)
	require.NoError(t, err)

	//testutils.SaveJsonFile(dest, "mysql/testdata/r_rating_films.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/mysql/r_rating_films.json")
}

func TestSelectAndUnionInProjection(t *testing.T) {
	if sourceIsMariaDB() {
		return
	}

	expectedSQL := `
SELECT payment.payment_id AS "payment.payment_id",
     (
          SELECT customer.customer_id AS "customer.customer_id"
          FROM dvds.customer
          LIMIT ?
     ),
     (
          (
               SELECT payment.payment_id AS "payment.payment_id"
               FROM dvds.payment
               LIMIT ?
               OFFSET ?
          )
          UNION
          (
               SELECT payment.payment_id AS "payment.payment_id"
               FROM dvds.payment
               LIMIT ?
               OFFSET ?
          )
          LIMIT ?
     )
FROM dvds.payment
LIMIT ?;
`

	query := Payment.
		SELECT(
			Payment.PaymentID,
			Customer.SELECT(Customer.CustomerID).LIMIT(1),
			UNION(
				Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(10),
				Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(2),
			).LIMIT(1),
		).
		LIMIT(12)

	testutils.AssertStatementSql(t, query, expectedSQL, int64(1), int64(1), int64(10), int64(1), int64(2), int64(1), int64(12))

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestSelectUNION(t *testing.T) {
	expectedSQL := `
(
     SELECT payment.payment_id AS "payment.payment_id"
     FROM dvds.payment
     LIMIT ?
     OFFSET ?
)
UNION
(
     SELECT payment.payment_id AS "payment.payment_id"
     FROM dvds.payment
     LIMIT ?
     OFFSET ?
)
LIMIT ?;
`
	query := UNION(
		Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(10),
		Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(2),
	).LIMIT(1)

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, expectedSQL, int64(1), int64(10), int64(1), int64(2), int64(1))

	query2 := Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(10).
		UNION(Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(2)).LIMIT(1)

	testutils.AssertStatementSql(t, query2, expectedSQL, int64(1), int64(10), int64(1), int64(2), int64(1))

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestSelectUNION_ALL(t *testing.T) {
	expectedSQL := `
(
     SELECT payment.payment_id AS "payment.payment_id"
     FROM dvds.payment
     LIMIT ?
     OFFSET ?
)
UNION ALL
(
     SELECT payment.payment_id AS "payment.payment_id"
     FROM dvds.payment
     LIMIT ?
     OFFSET ?
)
ORDER BY "payment.payment_id"
LIMIT ?
OFFSET ?;
`
	query := UNION_ALL(
		Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(10),
		Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(2),
	).ORDER_BY(Payment.PaymentID).
		LIMIT(4).
		OFFSET(3)

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, expectedSQL, int64(1), int64(10), int64(1), int64(2), int64(4), int64(3))

	query2 := Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(10).
		UNION_ALL(Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(2)).
		ORDER_BY(Payment.PaymentID).
		LIMIT(4).
		OFFSET(3)

	testutils.AssertStatementSql(t, query2, expectedSQL, int64(1), int64(10), int64(1), int64(2), int64(4), int64(3))

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
FROM dvds.language
     INNER JOIN dvds.film ON (film.language_id = language.language_id)
     INNER JOIN dvds.film_actor ON (film_actor.film_id = film.film_id)
     INNER JOIN dvds.actor ON (actor.actor_id = film_actor.actor_id)
     LEFT JOIN dvds.inventory ON (inventory.film_id = film.film_id)
     LEFT JOIN dvds.rental ON (rental.inventory_id = inventory.inventory_id)
ORDER BY language.language_id ASC, film.film_id ASC, actor.actor_id ASC, inventory.inventory_id ASC, rental.rental_id ASC
LIMIT ?;
`
	for i := 0; i < 2; i++ {

		query := Language.
			INNER_JOIN(Film, Film.LanguageID.EQ(Language.LanguageID)).
			INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(Film.FilmID)).
			INNER_JOIN(Actor, Actor.ActorID.EQ(FilmActor.ActorID)).
			LEFT_JOIN(Inventory, Inventory.FilmID.EQ(Film.FilmID)).
			LEFT_JOIN(Rental, Rental.InventoryID.EQ(Inventory.InventoryID)).
			SELECT(
				FilmActor.AllColumns,
				Film.AllColumns,
				Language.AllColumns,
				Actor.AllColumns,
				Inventory.AllColumns,
				Rental.AllColumns,
			).
			ORDER_BY(
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
		//testutils.SaveJsonFile(dest, "./mysql/testdata/lang_film_actor_inventory_rental.json")
		testutils.AssertJSONFile(t, dest, "./testdata/results/mysql/lang_film_actor_inventory_rental.json")
	}
}

func getRowLockTestData() map[RowLock]string {
	if sourceIsMariaDB() {
		return map[RowLock]string{
			UPDATE(): "UPDATE",
		}
	}
	return map[RowLock]string{
		UPDATE(): "UPDATE",
		SHARE():  "SHARE",
	}
}

func TestRowLock(t *testing.T) {
	expectedSQL := `
SELECT *
FROM dvds.address
LIMIT 3
OFFSET 1
FOR`
	query := Address.
		SELECT(STAR).
		LIMIT(3).
		OFFSET(1)

	for lockType, lockTypeStr := range getRowLockTestData() {
		query.FOR(lockType)

		expectedQuery := expectedSQL + " " + lockTypeStr + ";\n"
		testutils.AssertDebugStatementSql(t, query, expectedQuery, int64(3), int64(1))

		tx, _ := db.Begin()

		_, err := query.Exec(tx)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)
	}

	for lockType, lockTypeStr := range getRowLockTestData() {
		query.FOR(lockType.NOWAIT())

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+lockTypeStr+" NOWAIT;\n", int64(3), int64(1))

		tx, _ := db.Begin()

		_, err := query.Exec(tx)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)
	}

	if sourceIsMariaDB() {
		return
	}

	for lockType, lockTypeStr := range getRowLockTestData() {
		query.FOR(lockType.SKIP_LOCKED())

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+lockTypeStr+" SKIP LOCKED;\n", int64(3), int64(1))

		tx, _ := db.Begin()

		_, err := query.Exec(tx)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)
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

func TestLockInShareMode(t *testing.T) {
	expectedSQL := `
SELECT *
FROM dvds.address
LIMIT 3
OFFSET 1
LOCK IN SHARE MODE;
`
	query := Address.
		SELECT(STAR).
		LIMIT(3).
		OFFSET(1).
		LOCK_IN_SHARE_MODE()

	testutils.AssertDebugStatementSql(t, query, expectedSQL)

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestWindowFunction(t *testing.T) {

	if sourceIsMariaDB() {
		return
	}

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
FROM dvds.payment
WHERE payment.payment_id < ?
GROUP BY payment.amount, payment.customer_id, payment.payment_date;
`
	query := Payment.
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
		).GROUP_BY(Payment.Amount, Payment.CustomerID, Payment.PaymentDate).
		WHERE(Payment.PaymentID.LT(Int(10)))

	//fmt.Println(query.Sql())

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
FROM dvds.payment
WHERE payment.payment_id < ?
WINDOW w1 AS (PARTITION BY payment.payment_date), w2 AS (w1), w3 AS (w2 ORDER BY payment.customer_id)
ORDER BY payment.customer_id;
`
	query := Payment.SELECT(
		AVG(Payment.Amount).OVER(),
		AVG(Payment.Amount).OVER(Window("w1")),
		AVG(Payment.Amount).OVER(
			Window("w2").
				ORDER_BY(Payment.CustomerID).
				RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED)),
		),
		AVG(Payment.Amount).OVER(Window("w3").RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED))),
	).
		WHERE(Payment.PaymentID.LT(Int(10))).
		WINDOW("w1").AS(PARTITION_BY(Payment.PaymentDate)).
		WINDOW("w2").AS(Window("w1")).
		WINDOW("w3").AS(Window("w2").ORDER_BY(Payment.CustomerID)).
		ORDER_BY(Payment.CustomerID)

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, expectedSQL, int64(10))

	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestSimpleView(t *testing.T) {
	query := SELECT(
		view.ActorInfo.AllColumns,
	).
		FROM(view.ActorInfo).
		ORDER_BY(view.ActorInfo.ActorID).
		LIMIT(10)

	type ActorInfo struct {
		ActorID   int
		FirstName string
		LastName  string
		FilmInfo  string
	}

	var dest []ActorInfo

	err := query.Query(db, &dest)
	require.NoError(t, err)

	require.Equal(t, len(dest), 10)
	testutils.AssertJSON(t, dest[1:2], `
[
	{
		"ActorID": 2,
		"FirstName": "NICK",
		"LastName": "WAHLBERG",
		"FilmInfo": "Action: BULL SHAWSHANK; Animation: FIGHT JAWBREAKER; Children: JERSEY SASSY; Classics: DRACULA CRYSTAL, GILBERT PELICAN; Comedy: MALLRATS UNITED, RUSHMORE MERMAID; Documentary: ADAPTATION HOLES; Drama: WARDROBE PHANTOM; Family: APACHE DIVINE, CHISUM BEHAVIOR, INDIAN LOVE, MAGUIRE APACHE; Foreign: BABY HALL, HAPPINESS UNITED; Games: ROOF CHAMPION; Music: LUCKY FLYING; New: DESTINY SATURDAY, FLASH WARS, JEKYLL FROGMEN, MASK PEACH; Sci-Fi: CHAINSAW UPTOWN, GOODFELLAS SALUTE; Travel: LIAISONS SWEET, SMILE EARRING"
	}
]
`)
}

func TestJoinViewWithTable(t *testing.T) {
	query := SELECT(
		view.CustomerList.AllColumns,
		Rental.AllColumns,
	).
		FROM(view.CustomerList.
			INNER_JOIN(Rental, view.CustomerList.ID.EQ(Rental.CustomerID)),
		).
		ORDER_BY(view.CustomerList.ID).
		WHERE(view.CustomerList.ID.LT_EQ(Int(2)))

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
FROM dvds.customer
LIMIT 3;
`)
	var dest []model.Customer
	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	require.Equal(t, len(dest), 3)
}

func Test_SchemaRename(t *testing.T) {
	Film := Film.FromSchema("dvds2")
	Language := Language.FromSchema("dvds2")

	stmt := SELECT(
		Film.FilmID,
		Film.Title,
		Language.LanguageID,
		Language.Name,
	).FROM(
		Language.
			INNER_JOIN(Film, Film.LanguageID.EQ(Language.LanguageID)),
	).WHERE(
		Language.LanguageID.EQ(Int(1)),
	).ORDER_BY(
		Language.LanguageID, Film.FilmID,
	).LIMIT(5)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT film.film_id AS "film.film_id",
     film.title AS "film.title",
     language.language_id AS "language.language_id",
     language.name AS "language.name"
FROM dvds2.language
     INNER JOIN dvds2.film ON (film.language_id = language.language_id)
WHERE language.language_id = 1
ORDER BY language.language_id, film.film_id
LIMIT 5;
`)

	dest := struct {
		model.Language
		Films []model.Film
	}{}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	require.Len(t, dest.Films, 5)
	require.Equal(t, dest.Films[0].Title, "ACADEMY DINOSAUR")
	require.Equal(t, dest.Films[1].Title, "ACE GOLDFINGER")
	require.Equal(t, dest.Films[4].Title, "AFRICAN EGG")
}

func TestUseSchema(t *testing.T) {
	UseSchema("dvds2")
	defer UseSchema("dvds")

	stmt := SELECT(
		Film.FilmID,
		Film.Title,
		Film.LanguageID,
	).FROM(
		Film,
	).WHERE(
		Film.Title.EQ(String("AFRICAN EGG")),
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT film.film_id AS "film.film_id",
     film.title AS "film.title",
     film.language_id AS "film.language_id"
FROM dvds2.film
WHERE film.title = 'AFRICAN EGG';
`)

	var dest model.Film
	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	require.Equal(t, dest.FilmID, uint16(5))
	require.Equal(t, dest.Title, "AFRICAN EGG")
	require.Equal(t, dest.LanguageID, uint8(1))
}

func TestLateral(t *testing.T) {
	skipForMariaDB(t) // MariaDB does not implement LATERAL

	languages := LATERAL(
		SELECT(
			Language.AllColumns,
		).FROM(
			Language,
		).WHERE(
			Language.Name.NOT_IN(String("spanish")).
				AND(Film.LanguageID.EQ(Language.LanguageID)),
		),
	).AS("films")

	stmt := SELECT(
		Film.FilmID,
		Film.Title,
		languages.AllColumns(),
	).FROM(
		Film.CROSS_JOIN(languages),
	).WHERE(
		Film.FilmID.EQ(Int(1)),
	).ORDER_BY(
		Film.FilmID,
	).LIMIT(1)

	testutils.AssertDebugStatementSql(t, stmt, strings.Replace(`
SELECT film.film_id AS "film.film_id",
     film.title AS "film.title",
     films.''language.language_id'' AS "language.language_id",
     films.''language.name'' AS "language.name",
     films.''language.last_update'' AS "language.last_update"
FROM dvds.film
     CROSS JOIN LATERAL (
          SELECT language.language_id AS "language.language_id",
               language.name AS "language.name",
               language.last_update AS "language.last_update"
          FROM dvds.language
          WHERE (language.name NOT IN ('spanish')) AND (film.language_id = language.language_id)
     ) AS films
WHERE film.film_id = 1
ORDER BY film.film_id
LIMIT 1;
`, "''", "`", -1))

	type FilmLanguage struct {
		model.Film
		model.Language
	}

	var dest []FilmLanguage

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	require.Equal(t, dest[0].Film.Title, "ACADEMY DINOSAUR")
	require.Equal(t, dest[0].Language.Name, "English")

	t.Run("implicit cross join", func(t *testing.T) {
		stmt2 := SELECT(
			Film.FilmID,
			Film.Title,
			languages.AllColumns(),
		).FROM(
			Film,
			languages,
		).WHERE(
			Film.FilmID.EQ(Int(1)),
		).ORDER_BY(
			Film.FilmID,
		).LIMIT(1)

		testutils.AssertDebugStatementSql(t, stmt2, strings.Replace(`
SELECT film.film_id AS "film.film_id",
     film.title AS "film.title",
     films.''language.language_id'' AS "language.language_id",
     films.''language.name'' AS "language.name",
     films.''language.last_update'' AS "language.last_update"
FROM dvds.film,
     LATERAL (
          SELECT language.language_id AS "language.language_id",
               language.name AS "language.name",
               language.last_update AS "language.last_update"
          FROM dvds.language
          WHERE (language.name NOT IN ('spanish')) AND (film.language_id = language.language_id)
     ) AS films
WHERE film.film_id = 1
ORDER BY film.film_id
LIMIT 1;
`, "''", "`", -1))

		var dest2 []FilmLanguage

		err2 := stmt2.Query(db, &dest2)
		require.NoError(t, err2)
		require.Equal(t, dest, dest2)
	})
}

func TestRowsScan(t *testing.T) {

	stmt := SELECT(
		Inventory.AllColumns,
		Film.AllColumns,
		Store.AllColumns,
	).FROM(
		Inventory.
			INNER_JOIN(Film, Film.FilmID.EQ(Inventory.FilmID)).
			INNER_JOIN(Store, Store.StoreID.EQ(Inventory.StoreID)),
	).ORDER_BY(
		Inventory.InventoryID.ASC(),
	)

	rows, err := stmt.Rows(context.Background(), db)
	require.NoError(t, err)

	var inventory struct {
		model.Inventory

		Film  model.Film
		Store model.Store
	}

	for rows.Next() {
		err = rows.Scan(&inventory)
		require.NoError(t, err)

		require.NotEmpty(t, inventory.InventoryID)
		require.NotEmpty(t, inventory.FilmID)
		require.NotEmpty(t, inventory.StoreID)
		require.NotEmpty(t, inventory.LastUpdate)

		require.NotEmpty(t, inventory.Film.FilmID)
		require.NotEmpty(t, inventory.Film.Title)
		require.NotEmpty(t, inventory.Film.Description)

		require.NotEmpty(t, inventory.Store.StoreID)
		require.NotEmpty(t, inventory.Store.AddressID)
		require.NotEmpty(t, inventory.Store.ManagerStaffID)

		if inventory.InventoryID == 2103 {
			require.Equal(t, inventory.FilmID, uint16(456))
			require.Equal(t, inventory.StoreID, uint8(2))
			require.Equal(t, inventory.LastUpdate.Format(time.RFC3339), "2006-02-15T05:09:17Z")

			require.Equal(t, inventory.Film.FilmID, uint16(456))
			require.Equal(t, inventory.Film.Title, "INCH JET")
			require.Equal(t, *inventory.Film.Description, "A Fateful Saga of a Womanizer And a Student who must Defeat a Butler in A Monastery")
			require.Equal(t, *inventory.Film.ReleaseYear, int16(2006))

			require.Equal(t, inventory.Store.StoreID, uint8(2))
			require.Equal(t, inventory.Store.ManagerStaffID, uint8(2))
			require.Equal(t, inventory.Store.AddressID, uint16(2))
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

	numeric := CAST(Decimal("1234567890.111")).AS_DECIMAL()

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
	require.Equal(t, number.Float64, float64(1.23456789e+09))
}

// scan into custom base types should be equivalent to the scan into base go types
func TestScanIntoCustomBaseTypes(t *testing.T) {

	type MyUint8 uint8
	type MyUint16 uint16
	type MyUint32 uint32
	type MyInt16 int16
	type MyFloat32 float32
	type MyFloat64 float64
	type MyString string
	type MyTime = time.Time

	type film struct {
		FilmID             MyUint16 `sql:"primary_key"`
		Title              MyString
		Description        *MyString
		ReleaseYear        *MyInt16
		LanguageID         MyUint8
		OriginalLanguageID *MyUint8
		RentalDuration     MyUint8
		RentalRate         MyFloat32
		Length             *MyUint32
		ReplacementCost    MyFloat64
		Rating             *model.FilmRating
		SpecialFeatures    *MyString
		LastUpdate         MyTime
	}

	stmt := SELECT(
		Film.AllColumns,
	).FROM(
		Film,
	).ORDER_BY(
		Film.FilmID.ASC(),
	).LIMIT(3)

	var films []model.Film
	err := stmt.Query(db, &films)
	require.NoError(t, err)

	var myFilms []film
	err = stmt.Query(db, &myFilms)
	require.NoError(t, err)

	require.Equal(t, testutils.ToJSON(films), testutils.ToJSON(myFilms))
}

func TestConditionalFunctions(t *testing.T) {
	stmt := SELECT(
		EXISTS(
			Film.SELECT(Film.FilmID).WHERE(Film.RentalDuration.GT(Int(100))),
		).AS("exists"),
		CASE(Film.Length.GT(Int(120))).
			WHEN(Bool(true)).THEN(String("long film")).
			ELSE(String("short film")).AS("case"),
		COALESCE(Film.Description, String("none")).AS("coalesce"),
		NULLIF(Film.ReleaseYear, Int(200)).AS("null_if"),
		GREATEST(Film.RentalDuration, Int(4), Int(5)).AS("greatest"),
		LEAST(Film.RentalDuration, Int(7), Int(6)).AS("least"),
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
          FROM dvds.film
          WHERE film.rental_duration > 100
     )) AS "exists",
     (CASE (film.length > 120) WHEN TRUE THEN 'long film' ELSE 'short film' END) AS "case",
     COALESCE(film.description, 'none') AS "coalesce",
     NULLIF(film.release_year, 200) AS "null_if",
     GREATEST(film.rental_duration, 4, 5) AS "greatest",
     LEAST(film.rental_duration, 7, 6) AS "least"
FROM dvds.film
WHERE film.film_id < 5
ORDER BY film.film_id;
`)

	var res []struct {
		Exists   string
		Case     string
		Coalesce string
		NullIf   string
		Greatest string
		Least    string
	}

	err := stmt.Query(db, &res)
	require.NoError(t, err)

	testutils.AssertJSON(t, res, `
[
	{
		"Exists": "0",
		"Case": "short film",
		"Coalesce": "A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies",
		"NullIf": "2006",
		"Greatest": "6",
		"Least": "6"
	},
	{
		"Exists": "0",
		"Case": "short film",
		"Coalesce": "A Astounding Epistle of a Database Administrator And a Explorer who must Find a Car in Ancient China",
		"NullIf": "2006",
		"Greatest": "5",
		"Least": "3"
	},
	{
		"Exists": "0",
		"Case": "short film",
		"Coalesce": "A Astounding Reflection of a Lumberjack And a Car who must Sink a Lumberjack in A Baloon Factory",
		"NullIf": "2006",
		"Greatest": "7",
		"Least": "6"
	},
	{
		"Exists": "0",
		"Case": "short film",
		"Coalesce": "A Fanciful Documentary of a Frisbee And a Lumberjack who must Chase a Monkey in A Shark Tank",
		"NullIf": "2006",
		"Greatest": "5",
		"Least": "5"
	}
]
`)
}

func TestSelectOptimizerHints(t *testing.T) {

	stmt := SELECT(Actor.AllColumns).
		OPTIMIZER_HINTS(MAX_EXECUTION_TIME(1), QB_NAME("mainQueryBlock"), "NO_ICP(actor)").
		DISTINCT().
		FROM(Actor)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT /*+ MAX_EXECUTION_TIME(1) QB_NAME(mainQueryBlock) NO_ICP(actor) */ DISTINCT actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update"
FROM dvds.actor;
`)

	var actors []model.Actor

	err := stmt.QueryContext(context.Background(), db, &actors)
	require.NoError(t, err)
	require.Len(t, actors, 200)
}
