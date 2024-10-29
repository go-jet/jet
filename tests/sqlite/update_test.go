package sqlite

import (
	"context"
	"github.com/go-jet/jet/v2/qrm"
	model2 "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/model"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/table"
	"testing"
	"time"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/table"
	"github.com/stretchr/testify/require"
)

func TestUpdateValues(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	var expectedSQL = `
UPDATE link
SET name = 'Bong',
    url = 'http://bong.com'
WHERE link.name = 'Bing';
`
	t.Run("old version", func(t *testing.T) {
		query := Link.UPDATE(Link.Name, Link.URL).
			SET("Bong", "http://bong.com").
			WHERE(Link.Name.EQ(String("Bing")))

		testutils.AssertDebugStatementSql(t, query, expectedSQL, "Bong", "http://bong.com", "Bing")
		testutils.AssertExec(t, query, tx)
		requireLogged(t, query)
	})

	t.Run("new version", func(t *testing.T) {
		stmt := Link.UPDATE().
			SET(
				Link.Name.SET(String("Bong")),
				Link.URL.SET(String("http://bong.com")),
			).
			WHERE(Link.Name.EQ(String("Bing")))

		testutils.AssertDebugStatementSql(t, stmt, expectedSQL, "Bong", "http://bong.com", "Bing")
		testutils.AssertExec(t, stmt, tx)
		requireLogged(t, stmt)
	})

	links := []model.Link{}

	err := SELECT(Link.AllColumns).
		FROM(Link).
		WHERE(Link.Name.EQ(String("Bong"))).
		Query(tx, &links)

	require.NoError(t, err)
	require.Equal(t, len(links), 1)
	testutils.AssertDeepEqual(t, links[0], model.Link{
		ID:   24,
		URL:  "http://bong.com",
		Name: "Bong",
	})
}

func TestUpdateWithSubQueries(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	expectedSQL := `
UPDATE link
SET name = ?,
    url = (
         SELECT link.url AS "link.url"
         FROM link
         WHERE link.name = ?
    )
WHERE link.name = ?;
`
	t.Run("old version", func(t *testing.T) {
		query := Link.
			UPDATE(Link.Name, Link.URL).
			SET(
				String("Bong"),
				SELECT(Link.URL).
					FROM(Link).
					WHERE(Link.Name.EQ(String("Ask"))),
			).
			WHERE(Link.Name.EQ(String("Bing")))

		testutils.AssertStatementSql(t, query, expectedSQL, "Bong", "Ask", "Bing")
		testutils.AssertExec(t, query, tx)
		requireLogged(t, query)
	})

	t.Run("new version", func(t *testing.T) {
		query := Link.
			UPDATE().
			SET(
				Link.Name.SET(String("Bong")),
				Link.URL.SET(StringExp(
					SELECT(Link.URL).
						FROM(Link).
						WHERE(Link.Name.EQ(String("Ask"))),
				)),
			).
			WHERE(Link.Name.EQ(String("Bing")))

		testutils.AssertStatementSql(t, query, expectedSQL, "Bong", "Ask", "Bing")
		testutils.AssertExec(t, query, tx)
		requireLogged(t, query)
	})
}

func TestUpdateWithModelDataAndReturning(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	link := model.Link{
		ID:   20,
		URL:  "http://www.duckduckgo.com",
		Name: "DuckDuckGo",
	}

	stmt := Link.UPDATE(Link.AllColumns).
		MODEL(link).
		WHERE(Link.ID.EQ(Int32(link.ID))).
		RETURNING(
			Link.AllColumns,
			String("str").AS("dest.literal"),
			NOT(Bool(false)).AS("dest.unary_operator"),
			Link.ID.ADD(Int(11)).AS("dest.binary_operator"),
			CAST(Link.ID).AS_TEXT().AS("dest.cast_operator"),
			Link.Name.LIKE(String("Bing")).AS("dest.like_operator"),
			Link.Description.IS_NULL().AS("dest.is_null"),
			CASE(Link.Name).
				WHEN(String("Yahoo")).THEN(String("search")).
				WHEN(String("GMail")).THEN(String("mail")).
				ELSE(String("unknown")).AS("dest.case_operator"),
		)

	expectedSQL := `
UPDATE link
SET id = ?,
    url = ?,
    name = ?,
    description = ?
WHERE link.id = ?
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description",
          ? AS "dest.literal",
          (NOT ?) AS "dest.unary_operator",
          (link.id + ?) AS "dest.binary_operator",
          CAST(link.id AS TEXT) AS "dest.cast_operator",
          (link.name LIKE ?) AS "dest.like_operator",
          link.description IS NULL AS "dest.is_null",
          (CASE link.name WHEN ? THEN ? WHEN ? THEN ? ELSE ? END) AS "dest.case_operator";
`
	testutils.AssertStatementSql(t, stmt, expectedSQL, int32(20), "http://www.duckduckgo.com", "DuckDuckGo", nil, int32(20),
		"str", false, int64(11), "Bing", "Yahoo", "search", "GMail", "mail", "unknown")

	type Dest struct {
		model.Link
		Literal        string
		UnaryOperator  bool
		BinaryOperator int64
		CastOperator   string
		LikeOperator   bool
		IsNull         bool
		CaseOperator   string
	}

	var dest Dest

	err := stmt.Query(tx, &dest)
	require.NoError(t, err)
	require.EqualValues(t, dest, Dest{
		Link:           link,
		Literal:        "str",
		UnaryOperator:  true,
		BinaryOperator: 31,
		CastOperator:   "20",
		LikeOperator:   false,
		IsNull:         true,
		CaseOperator:   "unknown",
	})
	requireLogged(t, stmt)
}

func TestUpdateWithModelDataAndPredefinedColumnList(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	link := model.Link{
		ID:   20,
		URL:  "http://www.duckduckgo.com",
		Name: "DuckDuckGo",
	}

	updateColumnList := ColumnList{Link.Description, Link.Name, Link.URL}

	stmt := Link.UPDATE(updateColumnList).
		MODEL(link).
		WHERE(Link.ID.EQ(Int32(link.ID)))

	var expectedSQL = `
UPDATE link
SET description = NULL,
    name = 'DuckDuckGo',
    url = 'http://www.duckduckgo.com'
WHERE link.id = 20;
`

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, nil, "DuckDuckGo", "http://www.duckduckgo.com", int32(20))

	testutils.AssertExec(t, stmt, tx, 1)
	requireLogged(t, stmt)
}

func TestUpdateWithModelDataAndMutableColumns(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	link := model.Link{
		ID:   201,
		URL:  "http://www.duckduckgo.com",
		Name: "DuckDuckGo",
	}

	stmt := Link.UPDATE(Link.MutableColumns).
		MODEL(link).
		WHERE(Link.ID.EQ(Int32(link.ID)))

	var expectedSQL = `
UPDATE link
SET url = 'http://www.duckduckgo.com',
    name = 'DuckDuckGo',
    description = NULL
WHERE link.id = 201;
`

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, "http://www.duckduckgo.com", "DuckDuckGo", nil, int32(201))
	testutils.AssertExec(t, stmt, tx)
}

func TestUpdateWithInvalidModelData(t *testing.T) {
	defer func() {
		r := recover()
		require.Equal(t, r, "missing struct field for column : id")
	}()

	link := struct {
		Ident       int
		URL         string
		Name        string
		Description *string
		Rel         *string
	}{
		Ident: 201,
		URL:   "http://www.duckduckgo.com",
		Name:  "DuckDuckGo",
	}

	stmt := Link.UPDATE(Link.AllColumns).
		MODEL(link).
		WHERE(Link.ID.EQ(Int(int64(link.Ident))))

	stmt.Sql()
}

func TestUpdateContextDeadlineExceeded(t *testing.T) {

	updateStmt := Link.UPDATE(Link.Name, Link.URL).
		SET("Bong", "http://bong.com").
		WHERE(Link.Name.EQ(String("Bing")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var dest []model.Link
		err := updateStmt.QueryContext(ctx, tx, &dest)
		require.Error(t, err, "context deadline exceeded")
	})

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		_, err := updateStmt.ExecContext(ctx, tx)
		require.Error(t, err, "context deadline exceeded")
	})
}

func TestUpdateFrom(t *testing.T) {
	tx := beginDBTx(t)
	defer tx.Rollback()

	stmt := table.Rental.UPDATE().
		SET(
			table.Rental.RentalDate.SET(DateTime(2020, 2, 2, 0, 0, 0)),
		).
		FROM(
			table.Staff.
				INNER_JOIN(table.Store, table.Store.StoreID.EQ(table.Staff.StaffID)),
		).
		WHERE(
			table.Staff.StaffID.EQ(table.Rental.StaffID).
				AND(table.Staff.StaffID.EQ(Int(2))).
				AND(table.Rental.RentalID.LT(Int(10))),
		).
		RETURNING(
			table.Rental.AllColumns.Except(table.Rental.LastUpdate),
		)

	testutils.AssertDebugStatementSql(t, stmt, `
UPDATE rental
SET rental_date = DATETIME('2020-02-02 00:00:00')
FROM staff
     INNER JOIN store ON (store.store_id = staff.staff_id)
WHERE ((staff.staff_id = rental.staff_id) AND (staff.staff_id = 2)) AND (rental.rental_id < 10)
RETURNING rental.rental_id AS "rental.rental_id",
          rental.rental_date AS "rental.rental_date",
          rental.inventory_id AS "rental.inventory_id",
          rental.customer_id AS "rental.customer_id",
          rental.return_date AS "rental.return_date",
          rental.staff_id AS "rental.staff_id";
`)

	var dest []model2.Rental

	err := stmt.Query(tx, &dest)

	require.NoError(t, err)
	require.Len(t, dest, 3)
	testutils.AssertJSON(t, dest, `
[
	{
		"RentalID": 4,
		"RentalDate": "2020-02-02T00:00:00Z",
		"InventoryID": 2452,
		"CustomerID": 333,
		"ReturnDate": "2005-06-03T01:43:41Z",
		"StaffID": 2,
		"LastUpdate": "0001-01-01T00:00:00Z"
	},
	{
		"RentalID": 7,
		"RentalDate": "2020-02-02T00:00:00Z",
		"InventoryID": 3995,
		"CustomerID": 269,
		"ReturnDate": "2005-05-29T20:34:53Z",
		"StaffID": 2,
		"LastUpdate": "0001-01-01T00:00:00Z"
	},
	{
		"RentalID": 8,
		"RentalDate": "2020-02-02T00:00:00Z",
		"InventoryID": 2346,
		"CustomerID": 239,
		"ReturnDate": "2005-05-27T23:33:46Z",
		"StaffID": 2,
		"LastUpdate": "0001-01-01T00:00:00Z"
	}
]
`)
}
