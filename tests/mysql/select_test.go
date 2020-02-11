package mysql

import (
	"github.com/go-jet/jet/internal/testutils"
	. "github.com/go-jet/jet/mysql"
	"github.com/go-jet/jet/tests/.gentestdata/mysql/dvds/enum"
	"github.com/go-jet/jet/tests/.gentestdata/mysql/dvds/model"
	. "github.com/go-jet/jet/tests/.gentestdata/mysql/dvds/table"
	"github.com/go-jet/jet/tests/.gentestdata/mysql/dvds/view"
	"github.com/stretchr/testify/assert"

	"testing"
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

	assert.NoError(t, err)

	testutils.AssertDeepEqual(t, actor, actor2)
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
	dest := []model.Actor{}

	err := query.Query(db, &dest)

	assert.NoError(t, err)

	assert.Equal(t, len(dest), 200)
	testutils.AssertDeepEqual(t, dest[1], actor2)

	//testutils.PrintJson(dest)
	//testutils.SaveJsonFile(dest, "mysql/testdata/all_actors.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/mysql/all_actors.json")
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

	assert.NoError(t, err)

	//testutils.PrintJson(dest)

	assert.Equal(t, len(dest), 174)

	//testutils.SaveJsonFile(dest, "mysql/testdata/customer_payment_sum.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/mysql/customer_payment_sum.json")
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
	assert.NoError(t, err)

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
	assert.NoError(t, err)
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
	assert.NoError(t, err)
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
	assert.NoError(t, err)
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

		assert.NoError(t, err)
		//assert.Equal(t, len(dest), 1)
		//assert.Equal(t, len(dest[0].Films), 10)
		//assert.Equal(t, len(dest[0].Films[0].Actors), 10)

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
		assert.NoError(t, err)

		err = tx.Rollback()
		assert.NoError(t, err)
	}

	for lockType, lockTypeStr := range getRowLockTestData() {
		query.FOR(lockType.NOWAIT())

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+lockTypeStr+" NOWAIT;\n", int64(3), int64(1))

		tx, _ := db.Begin()

		_, err := query.Exec(tx)
		assert.NoError(t, err)

		err = tx.Rollback()
		assert.NoError(t, err)
	}

	if sourceIsMariaDB() {
		return
	}

	for lockType, lockTypeStr := range getRowLockTestData() {
		query.FOR(lockType.SKIP_LOCKED())

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+lockTypeStr+" SKIP LOCKED;\n", int64(3), int64(1))

		tx, _ := db.Begin()

		_, err := query.Exec(tx)
		assert.NoError(t, err)

		err = tx.Rollback()
		assert.NoError(t, err)
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
	assert.NoError(t, err)
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
	assert.NoError(t, err)
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
	assert.NoError(t, err)
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

	assert.NoError(t, err)
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
	assert.NoError(t, err)

	assert.Equal(t, len(dest), 10)
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
	assert.NoError(t, err)

	assert.Equal(t, len(dest), 2)
	assert.Equal(t, len(dest[0].Rentals), 32)
	assert.Equal(t, len(dest[1].Rentals), 27)
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
	assert.NoError(t, err)

	assert.Equal(t, len(dest), 3)
}
