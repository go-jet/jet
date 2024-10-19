package sqlite

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"

	. "github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/table"
)

func TestVALUES(t *testing.T) {

	values := VALUES(
		ROW(Int32(1), Int32(2), Float(4.666), Bool(false), String("txt")),
		ROW(Int32(11).ADD(Int32(2)), Int32(22), Float(33.222), Bool(true), String("png")),
		ROW(Int32(11), Int32(22), Float(33.222), Bool(true), NULL),
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
          VALUES (?, ?, ?, ?, ?),
                 (? + ?, ?, ?, ?, ?),
                 (?, ?, ?, ?, NULL)
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

	lastUpdate := DateTime(2007, time.February, 11, 12, 0, 0)

	films := VALUES(
		ROW(String("Chamber Italian"), Int64(117), Int32(2005), Float(5.82), lastUpdate),
		ROW(String("Grosse Wonderful"), Int64(49), Int32(2004), Float(6.242), lastUpdate),
		ROW(String("Airport Pollock"), Int64(54), Int32(2001), Float(7.22), NULL),
		ROW(String("Bright Encounters"), Int64(73), Int32(2002), Float(8.25), NULL),
		ROW(String("Academy Dinosaur"), Int64(83), Int32(2010), Float(9.22), DATETIME(lastUpdate, YEARS(2))),
	).AS("film_values")

	title := StringColumn("column1").From(films)
	releaseYear := IntegerColumn("column3").From(films)
	rentalRate := FloatColumn("column4").From(films)

	stmt := SELECT(
		Film.AllColumns,
		films.AllColumns(),
	).FROM(
		Film.
			INNER_JOIN(films, LOWER(title).EQ(LOWER(Film.Title))),
	).WHERE(AND(
		CAST(Film.ReleaseYear).AS_INTEGER().GT(releaseYear),
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
     film.original_language_id AS "film.original_language_id",
     film.rental_duration AS "film.rental_duration",
     film.rental_rate AS "film.rental_rate",
     film.length AS "film.length",
     film.replacement_cost AS "film.replacement_cost",
     film.rating AS "film.rating",
     film.special_features AS "film.special_features",
     film.last_update AS "film.last_update",
     film_values.column1 AS "column1",
     film_values.column2 AS "column2",
     film_values.column3 AS "column3",
     film_values.column4 AS "column4",
     film_values.column5 AS "column5"
FROM film
     INNER JOIN (
          VALUES ('Chamber Italian', 117, 2005, 5.82, DATETIME('2007-02-11 12:00:00')),
                 ('Grosse Wonderful', 49, 2004, 6.242, DATETIME('2007-02-11 12:00:00')),
                 ('Airport Pollock', 54, 2001, 7.22, NULL),
                 ('Bright Encounters', 73, 2002, 8.25, NULL),
                 ('Academy Dinosaur', 83, 2010, 9.22, DATETIME(DATETIME('2007-02-11 12:00:00'), '2 YEARS'))
     ) AS film_values ON (LOWER(film_values.column1) = LOWER(film.title))
WHERE (
          (CAST(film.release_year AS INTEGER) > film_values.column3)
              AND (film.rental_rate < film_values.column4)
      )
ORDER BY film_values.column1;
`)

	var dest []struct {
		Film model.Film

		Column1 string
		Column2 int
		Column3 int
		Column4 float32
		Column5 *time.Time
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertJSON(t, dest, `
[
	{
		"Film": {
			"FilmID": 8,
			"Title": "AIRPORT POLLOCK",
			"Description": "A Epic Tale of a Moose And a Girl who must Confront a Monkey in Ancient India",
			"ReleaseYear": "2006",
			"LanguageID": 1,
			"OriginalLanguageID": null,
			"RentalDuration": 6,
			"RentalRate": 4.99,
			"Length": 54,
			"ReplacementCost": 15.99,
			"Rating": "R",
			"SpecialFeatures": "Trailers",
			"LastUpdate": "2019-04-11T18:11:48Z"
		},
		"Column1": "Airport Pollock",
		"Column2": 54,
		"Column3": 2001,
		"Column4": 7.22,
		"Column5": null
	},
	{
		"Film": {
			"FilmID": 98,
			"Title": "BRIGHT ENCOUNTERS",
			"Description": "A Fateful Yarn of a Lumberjack And a Feminist who must Conquer a Student in A Jet Boat",
			"ReleaseYear": "2006",
			"LanguageID": 1,
			"OriginalLanguageID": null,
			"RentalDuration": 4,
			"RentalRate": 4.99,
			"Length": 73,
			"ReplacementCost": 12.99,
			"Rating": "PG-13",
			"SpecialFeatures": "Trailers",
			"LastUpdate": "2019-04-11T18:11:48Z"
		},
		"Column1": "Bright Encounters",
		"Column2": 73,
		"Column3": 2002,
		"Column4": 8.25,
		"Column5": null
	},
	{
		"Film": {
			"FilmID": 133,
			"Title": "CHAMBER ITALIAN",
			"Description": "A Fateful Reflection of a Moose And a Husband who must Overcome a Monkey in Nigeria",
			"ReleaseYear": "2006",
			"LanguageID": 1,
			"OriginalLanguageID": null,
			"RentalDuration": 7,
			"RentalRate": 4.99,
			"Length": 117,
			"ReplacementCost": 14.99,
			"Rating": "NC-17",
			"SpecialFeatures": "Trailers",
			"LastUpdate": "2019-04-11T18:11:48Z"
		},
		"Column1": "Chamber Italian",
		"Column2": 117,
		"Column3": 2005,
		"Column4": 5.82,
		"Column5": "2007-02-11T12:00:00Z"
	},
	{
		"Film": {
			"FilmID": 384,
			"Title": "GROSSE WONDERFUL",
			"Description": "A Epic Drama of a Cat And a Explorer who must Redeem a Moose in Australia",
			"ReleaseYear": "2006",
			"LanguageID": 1,
			"OriginalLanguageID": null,
			"RentalDuration": 5,
			"RentalRate": 4.99,
			"Length": 49,
			"ReplacementCost": 19.99,
			"Rating": "R",
			"SpecialFeatures": "Behind the Scenes",
			"LastUpdate": "2019-04-11T18:11:48Z"
		},
		"Column1": "Grosse Wonderful",
		"Column2": 49,
		"Column3": 2004,
		"Column4": 6.242,
		"Column5": "2007-02-11T12:00:00Z"
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
				ROW(Int32(204), Float(1.21)),
				ROW(Int32(207), Float(1.02)),
				ROW(Int32(200), Float(1.34)),
				ROW(Int32(203), Float(1.72)),
			),
		),
	)(
		Payment.UPDATE().
			SET(
				Payment.Amount.SET(Payment.Amount.MUL(increase)),
			).
			FROM(paymentsToUpdate).
			WHERE(Payment.PaymentID.EQ(paymentID)).
			RETURNING(Payment.AllColumns),
	)

	testutils.AssertStatementSql(t, stmt, strings.ReplaceAll(`
WITH values_cte (''payment_ID'', increase) AS (
     VALUES (?, ?),
            (?, ?),
            (?, ?),
            (?, ?)
)
UPDATE payment
SET amount = (payment.amount * values_cte.increase)
FROM values_cte
WHERE payment.payment_id = values_cte.''payment_ID''
RETURNING payment.payment_id AS "payment.payment_id",
          payment.customer_id AS "payment.customer_id",
          payment.staff_id AS "payment.staff_id",
          payment.rental_id AS "payment.rental_id",
          payment.amount AS "payment.amount",
          payment.payment_date AS "payment.payment_date",
          payment.last_update AS "payment.last_update";
`, "''", "`"))

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var payments []model.Payment

		err := stmt.Query(tx, &payments)

		require.NoError(t, err)
		testutils.AssertJSON(t, payments, `
[
	{
		"PaymentID": 200,
		"CustomerID": 7,
		"StaffID": 2,
		"RentalID": 11542,
		"Amount": 10.706600000000002,
		"PaymentDate": "2005-08-17T00:51:32Z",
		"LastUpdate": "2019-04-11T18:11:50Z"
	},
	{
		"PaymentID": 203,
		"CustomerID": 7,
		"StaffID": 2,
		"RentalID": 13373,
		"Amount": 5.1428,
		"PaymentDate": "2005-08-19T21:23:31Z",
		"LastUpdate": "2019-04-11T18:11:50Z"
	},
	{
		"PaymentID": 204,
		"CustomerID": 7,
		"StaffID": 1,
		"RentalID": 13476,
		"Amount": 3.6179,
		"PaymentDate": "2005-08-20T01:06:04Z",
		"LastUpdate": "2019-04-11T18:11:50Z"
	},
	{
		"PaymentID": 207,
		"CustomerID": 8,
		"StaffID": 2,
		"RentalID": 866,
		"Amount": 7.1298,
		"PaymentDate": "2005-05-30T03:43:54Z",
		"LastUpdate": "2019-04-11T18:11:50Z"
	}
]
`)
	})

}
