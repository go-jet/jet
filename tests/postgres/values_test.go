package postgres

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestVALUES(t *testing.T) {

	values := VALUES(
		WRAP(Int32(1), Int32(2), Real(4.666), Bool(false), String("txt")),
		WRAP(Int32(11).ADD(Int32(2)), Int32(22), Real(33.222), Bool(true), String("png")),
		WRAP(Int32(11), Int32(22), Real(33.222), Bool(true), NULL),
	).AS("values_table")

	stmt := SELECT(
		values.AllColumns(),
	).FROM(
		values,
	)

	testutils.AssertStatementSql(t, stmt, `
SELECT values_table.column1 AS "column1",
     values_table.column2 AS "column2",
     values_table.column3 AS "column3",
     values_table.column4 AS "column4",
     values_table.column5 AS "column5"
FROM (
          VALUES ($1::integer, $2::integer, $3::real, $4::boolean, $5::text),
                 ($6::integer + $7::integer, $8::integer, $9::real, $10::boolean, $11::text),
                 ($12::integer, $13::integer, $14::real, $15::boolean, NULL)
     ) AS values_table;
`)

	var dest []struct {
		Column1 int
		Column2 int
		Column3 float32
		Column4 bool
		Column5 *string
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertJSON(t, dest, `
[
	{
		"Column1": 1,
		"Column2": 2,
		"Column3": 4.666,
		"Column4": false,
		"Column5": "txt"
	},
	{
		"Column1": 13,
		"Column2": 22,
		"Column3": 33.222,
		"Column4": true,
		"Column5": "png"
	},
	{
		"Column1": 11,
		"Column2": 22,
		"Column3": 33.222,
		"Column4": true,
		"Column5": null
	}
]
`)
}

func TestVALUES_Join(t *testing.T) {

	title := StringColumn("title")
	releaseYear := IntegerColumn("ReleaseYear")
	rentalRate := FloatColumn("rental_rate")

	lastUpdate := Timestamp(2007, time.February, 11, 12, 0, 0)

	filmValues := VALUES(
		WRAP(String("Chamber Italian"), Int64(117), Int32(2005), Real(5.82), lastUpdate),
		WRAP(String("Grosse Wonderful"), Int64(49), Int32(2004), Real(6.242), lastUpdate.ADD(INTERVAL(1, HOUR))),
		WRAP(String("Airport Pollock"), Int64(54), Int32(2001), Real(7.22), NULL),
		WRAP(String("Bright Encounters"), Int64(73), Int32(2002), Real(8.25), NULL),
		WRAP(String("Academy Dinosaur"), Int64(83), Int32(2010), Real(9.22), lastUpdate.SUB(INTERVAL(2, MINUTE))),
	).AS("film_values",
		title, IntegerColumn("length"), releaseYear, rentalRate, TimestampColumn("update_date"))

	stmt := SELECT(
		Film.AllColumns,
		filmValues.AllColumns(),
	).FROM(
		Film.
			INNER_JOIN(filmValues, title.EQ(Film.Title)),
	).WHERE(AND(
		Film.ReleaseYear.GT(releaseYear),
		Film.RentalRate.LT(rentalRate),
	)).ORDER_BY(
		title,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT film.film_id AS "film.film_id",
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
     film_values.title AS "title",
     film_values.length AS "length",
     film_values."ReleaseYear" AS "ReleaseYear",
     film_values.rental_rate AS "rental_rate",
     film_values.update_date AS "update_date"
FROM dvds.film
     INNER JOIN (
          VALUES ('Chamber Italian'::text, 117::bigint, 2005::integer, 5.820000171661377::real, '2007-02-11 12:00:00'::timestamp without time zone),
                 ('Grosse Wonderful'::text, 49::bigint, 2004::integer, 6.242000102996826::real, '2007-02-11 12:00:00'::timestamp without time zone + INTERVAL '1 HOUR'),
                 ('Airport Pollock'::text, 54::bigint, 2001::integer, 7.21999979019165::real, NULL),
                 ('Bright Encounters'::text, 73::bigint, 2002::integer, 8.25::real, NULL),
                 ('Academy Dinosaur'::text, 83::bigint, 2010::integer, 9.220000267028809::real, '2007-02-11 12:00:00'::timestamp without time zone - INTERVAL '2 MINUTE')
     ) AS film_values (title, length, "ReleaseYear", rental_rate, update_date) ON (film_values.title = film.title)
WHERE (
          (film.release_year > film_values."ReleaseYear")
              AND (film.rental_rate < film_values.rental_rate)
      )
ORDER BY film_values.title;
`)

	//fmt.Println(stmt.DebugSql())

	var dest []struct {
		Film model.Film

		Title       string
		Length      int
		ReleaseYear int
		RentalRate  float32
		UpdateDate  *time.Time
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	assert.Len(t, dest, 4)
	testutils.AssertJSON(t, dest[0:2], `
[
	{
		"Film": {
			"FilmID": 8,
			"Title": "Airport Pollock",
			"Description": "A Epic Tale of a Moose And a Girl who must Confront a Monkey in Ancient India",
			"ReleaseYear": 2006,
			"LanguageID": 1,
			"RentalDuration": 6,
			"RentalRate": 4.99,
			"Length": 54,
			"ReplacementCost": 15.99,
			"Rating": "R",
			"LastUpdate": "2013-05-26T14:50:58.951Z",
			"SpecialFeatures": [
				"Trailers"
			],
			"Fulltext": "'airport':1 'ancient':18 'confront':14 'epic':4 'girl':11 'india':19 'monkey':16 'moos':8 'must':13 'pollock':2 'tale':5"
		},
		"Title": "Airport Pollock",
		"Length": 54,
		"ReleaseYear": 2001,
		"RentalRate": 7.22,
		"UpdateDate": null
	},
	{
		"Film": {
			"FilmID": 98,
			"Title": "Bright Encounters",
			"Description": "A Fateful Yarn of a Lumberjack And a Feminist who must Conquer a Student in A Jet Boat",
			"ReleaseYear": 2006,
			"LanguageID": 1,
			"RentalDuration": 4,
			"RentalRate": 4.99,
			"Length": 73,
			"ReplacementCost": 12.99,
			"Rating": "PG-13",
			"LastUpdate": "2013-05-26T14:50:58.951Z",
			"SpecialFeatures": [
				"Trailers"
			],
			"Fulltext": "'boat':20 'bright':1 'conquer':14 'encount':2 'fate':4 'feminist':11 'jet':19 'lumberjack':8 'must':13 'student':16 'yarn':5"
		},
		"Title": "Bright Encounters",
		"Length": 73,
		"ReleaseYear": 2002,
		"RentalRate": 8.25,
		"UpdateDate": null
	}
]
`)
}

func TestVALUES_CTE_Update(t *testing.T) {

	paymentID := IntegerColumn("payment_ID")
	increase := FloatColumn("increase")
	paymentsToUpdate := CTE("values_cte", paymentID, increase)

	stmt := WITH(
		paymentsToUpdate.AS(
			VALUES(
				WRAP(Int32(20564), Real(1.21)),
				WRAP(Int32(20567), Real(1.02)),
				WRAP(Int32(20570), Real(1.34)),
				WRAP(Int32(20573), Real(1.72)),
			),
		),
	)(
		Payment.UPDATE().
			SET(
				Payment.Amount.SET(Payment.Amount.MUL(CAST(increase).AS_DECIMAL())),
			).
			FROM(paymentsToUpdate).
			WHERE(Payment.PaymentID.EQ(paymentID)).
			RETURNING(Payment.AllColumns),
	)

	testutils.AssertDebugStatementSql(t, stmt, `
WITH values_cte ("payment_ID", increase) AS (
     VALUES (20564::integer, 1.2100000381469727::real),
            (20567::integer, 1.0199999809265137::real),
            (20570::integer, 1.340000033378601::real),
            (20573::integer, 1.7200000286102295::real)
)
UPDATE dvds.payment
SET amount = (payment.amount * values_cte.increase::decimal)
FROM values_cte
WHERE payment.payment_id = values_cte."payment_ID"
RETURNING payment.payment_id AS "payment.payment_id",
          payment.customer_id AS "payment.customer_id",
          payment.staff_id AS "payment.staff_id",
          payment.rental_id AS "payment.rental_id",
          payment.amount AS "payment.amount",
          payment.payment_date AS "payment.payment_date";
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {

		var payments []model.Payment

		err := stmt.Query(tx, &payments)
		require.NoError(t, err)

		assert.Len(t, payments, 4)
		testutils.AssertJSON(t, payments[0:2], `
[
	{
		"PaymentID": 20564,
		"CustomerID": 379,
		"StaffID": 2,
		"RentalID": 11457,
		"Amount": 4.83,
		"PaymentDate": "2007-03-02T19:42:42.996577Z"
	},
	{
		"PaymentID": 20567,
		"CustomerID": 379,
		"StaffID": 2,
		"RentalID": 13397,
		"Amount": 8.15,
		"PaymentDate": "2007-03-19T20:35:01.996577Z"
	}
]
`)
	})

}
