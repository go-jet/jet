package mysql

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"

	. "github.com/go-jet/jet/v2/mysql"

	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/table"
)

func TestVALUES(t *testing.T) {
	skipForMariaDB(t)

	valuesTable := VALUES(
		ROW(Int32(1), Int32(2), Float(4.666), Bool(false), String("txt")),
		ROW(Int32(11).ADD(Int32(2)), Int32(22), Float(33.222), Bool(true), String("png")),
		ROW(Int32(11), Int32(22), Float(33.222), Bool(true), NULL),
	).AS("values_table")

	stmt := SELECT(
		valuesTable.AllColumns(),
	).FROM(
		valuesTable,
	)

	testutils.AssertStatementSql(t, stmt, `
SELECT values_table.column_0 AS "column_0",
     values_table.column_1 AS "column_1",
     values_table.column_2 AS "column_2",
     values_table.column_3 AS "column_3",
     values_table.column_4 AS "column_4"
FROM (
          VALUES ROW(?, ?, ?, ?, ?),
                 ROW(? + ?, ?, ?, ?, ?),
                 ROW(?, ?, ?, ?, NULL)
     ) AS values_table;
`)

	var dest []struct {
		Column0 int
		Column1 int
		Column2 float32
		Column3 bool
		Column4 *string
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertJSON(t, dest, `
[
	{
		"Column0": 1,
		"Column1": 2,
		"Column2": 4.666,
		"Column3": false,
		"Column4": "txt"
	},
	{
		"Column0": 13,
		"Column1": 22,
		"Column2": 33.222,
		"Column3": true,
		"Column4": "png"
	},
	{
		"Column0": 11,
		"Column1": 22,
		"Column2": 33.222,
		"Column3": true,
		"Column4": null
	}
]
`)
}

func TestVALUES_Join(t *testing.T) {
	skipForMariaDB(t)

	title := StringColumn("title")
	releaseYear := IntegerColumn("ReleaseYear")
	rentalRate := FloatColumn("rental_rate")

	lastUpdate := Timestamp(2007, time.February, 11, 12, 0, 0)

	films := VALUES(
		ROW(String("Chamber Italian"), Int64(117), Int32(2005), Float(5.82), lastUpdate),
		ROW(String("Grosse Wonderful"), Int64(49), Int32(2004), Float(6.242), lastUpdate.ADD(INTERVAL(1, HOUR))),
		ROW(String("Airport Pollock"), Int64(54), Int32(2001), Float(7.22), NULL),
		ROW(String("Bright Encounters"), Int64(73), Int32(2002), Float(8.25), NULL),
		ROW(String("Academy Dinosaur"), Int64(83), Int32(2010), Float(9.22), lastUpdate.SUB(INTERVAL(2, MINUTE))),
	).AS("film_values",
		title, IntegerColumn("length"), releaseYear, rentalRate, TimestampColumn("last_update"))

	stmt := SELECT(
		Film.AllColumns,
		films.AllColumns(),
	).FROM(
		Film.
			INNER_JOIN(films, title.EQ(Film.Title)),
	).WHERE(AND(
		Film.ReleaseYear.GT(releaseYear),
		Film.RentalRate.LT(rentalRate),
	)).ORDER_BY(
		title,
	)

	testutils.AssertDebugStatementSql(t, stmt, strings.ReplaceAll(`
SELECT film.film_id AS "film.film_id",
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
     film_values.title AS "title",
     film_values.length AS "length",
     film_values.''ReleaseYear'' AS "ReleaseYear",
     film_values.rental_rate AS "rental_rate",
     film_values.last_update AS "last_update"
FROM dvds.film
     INNER JOIN (
          VALUES ROW('Chamber Italian', 117, 2005, 5.82, TIMESTAMP('2007-02-11 12:00:00')),
                 ROW('Grosse Wonderful', 49, 2004, 6.242, TIMESTAMP('2007-02-11 12:00:00') + INTERVAL 1 HOUR),
                 ROW('Airport Pollock', 54, 2001, 7.22, NULL),
                 ROW('Bright Encounters', 73, 2002, 8.25, NULL),
                 ROW('Academy Dinosaur', 83, 2010, 9.22, TIMESTAMP('2007-02-11 12:00:00') - INTERVAL 2 MINUTE)
     ) AS film_values (title, length, ''ReleaseYear'', rental_rate, last_update) ON (film_values.title = film.title)
WHERE (
          (film.release_year > film_values.''ReleaseYear'')
              AND (film.rental_rate < film_values.rental_rate)
      )
ORDER BY film_values.title;
`, "''", "`"))

	var dest []struct {
		Film model.Film

		Title       string
		Length      int
		ReleaseYear int
		RentalRate  float32
		LastUpdate  *time.Time
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	require.Len(t, dest, 4)
	testutils.AssertJSON(t, dest[0:2], `
[
	{
		"Film": {
			"FilmID": 8,
			"Title": "AIRPORT POLLOCK",
			"Description": "A Epic Tale of a Moose And a Girl who must Confront a Monkey in Ancient India",
			"ReleaseYear": 2006,
			"LanguageID": 1,
			"OriginalLanguageID": null,
			"RentalDuration": 6,
			"RentalRate": 4.99,
			"Length": 54,
			"ReplacementCost": 15.99,
			"Rating": "R",
			"SpecialFeatures": "Trailers",
			"LastUpdate": "2006-02-15T05:03:42Z"
		},
		"Title": "Airport Pollock",
		"Length": 54,
		"ReleaseYear": 2001,
		"RentalRate": 7.22,
		"LastUpdate": null
	},
	{
		"Film": {
			"FilmID": 98,
			"Title": "BRIGHT ENCOUNTERS",
			"Description": "A Fateful Yarn of a Lumberjack And a Feminist who must Conquer a Student in A Jet Boat",
			"ReleaseYear": 2006,
			"LanguageID": 1,
			"OriginalLanguageID": null,
			"RentalDuration": 4,
			"RentalRate": 4.99,
			"Length": 73,
			"ReplacementCost": 12.99,
			"Rating": "PG-13",
			"SpecialFeatures": "Trailers",
			"LastUpdate": "2006-02-15T05:03:42Z"
		},
		"Title": "Bright Encounters",
		"Length": 73,
		"ReleaseYear": 2002,
		"RentalRate": 8.25,
		"LastUpdate": null
	}
]
`)
}

func TestVALUES_CTE_Update(t *testing.T) {
	skipForMariaDB(t)

	paymentID := IntegerColumn("payment_id")
	increase := FloatColumn("increase")
	paymentsToUpdate := CTE("values_cte", paymentID, increase)

	stmt := WITH(
		paymentsToUpdate.AS(
			VALUES(
				ROW(Int32(204), Float(1.21)),
				ROW(Int32(207), Float(1.02)),
				ROW(Int32(200), Float(1.34)),
				ROW(Int32(203), Float(1.72)),
			),
		),
	)(
		Payment.INNER_JOIN(paymentsToUpdate, paymentID.EQ(Payment.PaymentID)).
			UPDATE().
			SET(
				Payment.Amount.SET(Payment.Amount.MUL(increase)),
			).WHERE(Bool(true)),
	)

	testutils.AssertStatementSql(t, stmt, `
WITH values_cte (payment_id, increase) AS (
     VALUES ROW(?, ?),
            ROW(?, ?),
            ROW(?, ?),
            ROW(?, ?)
)
UPDATE dvds.payment
INNER JOIN values_cte ON (values_cte.payment_id = payment.payment_id)
SET amount = (payment.amount * values_cte.increase)
WHERE ?;
`)

	testutils.AssertExecAndRollback(t, stmt, db, 4)
}

func TestVALUES_MariaDB(t *testing.T) {
	onlyMariaDB(t) // mariadb won't accept values rows if all the elements are placeholders, so we have to use raw statement

	paymentID := IntegerColumn("payment_id")
	increase := FloatColumn("increase")
	paymentsToUpdate := CTE("values_cte", paymentID, increase)

	stmt := WITH(
		paymentsToUpdate.AS(
			RawStatement(`
				 VALUES (204, 1.21),
						(207, 1.02),
						(200, 1.34),
						(203, 1.72)
			`),
		),
	)(
		SELECT(
			Payment.AllColumns,
			paymentsToUpdate.AllColumns(),
		).FROM(
			Payment.
				INNER_JOIN(paymentsToUpdate, paymentID.EQ(Payment.PaymentID)),
		).WHERE(
			increase.GT(Float(1.03)),
		).ORDER_BY(
			increase,
		),
	)

	testutils.AssertStatementSql(t, stmt, `
WITH values_cte (payment_id, increase) AS (
				 VALUES (204, 1.21),
						(207, 1.02),
						(200, 1.34),
						(203, 1.72)
			
)
SELECT payment.payment_id AS "payment.payment_id",
     payment.customer_id AS "payment.customer_id",
     payment.staff_id AS "payment.staff_id",
     payment.rental_id AS "payment.rental_id",
     payment.amount AS "payment.amount",
     payment.payment_date AS "payment.payment_date",
     payment.last_update AS "payment.last_update",
     values_cte.payment_id AS "payment_id",
     values_cte.increase AS "increase"
FROM dvds.payment
     INNER JOIN values_cte ON (values_cte.payment_id = payment.payment_id)
WHERE values_cte.increase > ?
ORDER BY values_cte.increase;
`)

	var dest []struct {
		model.Payment

		Increase float64
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertJSON(t, dest, `
[
	{
		"PaymentID": 204,
		"CustomerID": 7,
		"StaffID": 1,
		"RentalID": 13476,
		"Amount": 2.99,
		"PaymentDate": "2005-08-20T01:06:04Z",
		"LastUpdate": "2006-02-15T22:12:31Z",
		"Increase": 1.21
	},
	{
		"PaymentID": 200,
		"CustomerID": 7,
		"StaffID": 2,
		"RentalID": 11542,
		"Amount": 7.99,
		"PaymentDate": "2005-08-17T00:51:32Z",
		"LastUpdate": "2006-02-15T22:12:31Z",
		"Increase": 1.34
	},
	{
		"PaymentID": 203,
		"CustomerID": 7,
		"StaffID": 2,
		"RentalID": 13373,
		"Amount": 2.99,
		"PaymentDate": "2005-08-19T21:23:31Z",
		"LastUpdate": "2006-02-15T22:12:31Z",
		"Increase": 1.72
	}
]
`)
}
