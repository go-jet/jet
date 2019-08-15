package mysql

import (
	"github.com/go-jet/jet/internal/testutils"
	. "github.com/go-jet/jet/mysql"
	"github.com/go-jet/jet/tests/.gentestdata/mysql/dvds/enum"
	"github.com/go-jet/jet/tests/.gentestdata/mysql/dvds/model"
	. "github.com/go-jet/jet/tests/.gentestdata/mysql/dvds/table"
	"gotest.tools/assert"

	"testing"
)

func TestSelect_ScanToStruct(t *testing.T) {
	query := Actor.
		SELECT(Actor.AllColumns).
		DISTINCT().
		WHERE(Actor.ActorID.EQ(Int(1)))

	testutils.AssertStatementSql(t, query, `
SELECT DISTINCT actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update"
FROM dvds.actor
WHERE actor.actor_id = ?;
`, int64(1))

	actor := model.Actor{}
	err := query.Query(db, &actor)

	assert.NilError(t, err)

	assert.DeepEqual(t, actor, actor1)
}

var actor1 = model.Actor{
	ActorID:    1,
	FirstName:  "PENELOPE",
	LastName:   "GUINESS",
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
	dest := []model.Actor{}

	err := query.Query(db, &dest)

	assert.NilError(t, err)

	assert.Equal(t, len(dest), 200)
	assert.DeepEqual(t, dest[0], actor1)

	//testutils.PrintJson(dest)
	//testutils.SaveJsonFile(dest, "mysql/testdata/all_actors.json")
	testutils.AssertJSONFile(t, dest, "mysql/testdata/all_actors.json")
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
     MAX(payment.amount) AS "amount.max",
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
			MAXf(Payment.Amount).AS("amount.max"),
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

	assert.NilError(t, err)

	//testutils.PrintJson(dest)

	assert.Equal(t, len(dest), 174)

	//testutils.SaveJsonFile(dest, "mysql/testdata/customer_payment_sum.json")
	testutils.AssertJSONFile(t, dest, "mysql/testdata/customer_payment_sum.json")
}

func TestSubQuery(t *testing.T) {

	rRatingFilms := Film.
		SELECT(
			Film.FilmID,
			Film.Title,
			Film.Rating,
		).
		WHERE(Film.Rating.EQ(enum.FilmRating.R)).
		AsTable("rFilms")

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
	assert.NilError(t, err)

	//testutils.SaveJsonFile(dest, "mysql/testdata/r_rating_films.json")
	testutils.AssertJSONFile(t, dest, "mysql/testdata/r_rating_films.json")
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

	err := query.Query(db, &struct{}{})
	assert.NilError(t, err)
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

	err := query.Query(db, &struct{}{})
	assert.NilError(t, err)
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

	err := query.Query(db, &struct{}{})
	assert.NilError(t, err)
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

		//fmt.Println(query.Sql())

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

		assert.NilError(t, err)
		//assert.Equal(t, len(dest), 1)
		//assert.Equal(t, len(dest[0].Films), 10)
		//assert.Equal(t, len(dest[0].Films[0].Actors), 10)

		//testutils.SaveJsonFile(dest, "./mysql/testdata/lang_film_actor_inventory_rental.json")

		testutils.AssertJSONFile(t, dest, "./mysql/testdata/lang_film_actor_inventory_rental.json")
	}
}

func getRowLockTestData() map[SelectLock]string {
	if sourceIsMariaDB() {
		return map[SelectLock]string{
			UPDATE(): "UPDATE",
		}
	}
	return map[SelectLock]string{
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
		assert.NilError(t, err)

		err = tx.Rollback()
		assert.NilError(t, err)
	}

	for lockType, lockTypeStr := range getRowLockTestData() {
		query.FOR(lockType.NOWAIT())

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+lockTypeStr+" NOWAIT;\n", int64(3), int64(1))

		tx, _ := db.Begin()

		_, err := query.Exec(tx)
		assert.NilError(t, err)

		err = tx.Rollback()
		assert.NilError(t, err)
	}

	if sourceIsMariaDB() {
		return
	}

	for lockType, lockTypeStr := range getRowLockTestData() {
		query.FOR(lockType.SKIP_LOCKED())

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+lockTypeStr+" SKIP LOCKED;\n", int64(3), int64(1))

		tx, _ := db.Begin()

		_, err := query.Exec(tx)
		assert.NilError(t, err)

		err = tx.Rollback()
		assert.NilError(t, err)
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

	err := query.Query(db, &struct{}{})
	assert.NilError(t, err)
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

	err := query.Query(db, &struct{}{})
	assert.NilError(t, err)
}
