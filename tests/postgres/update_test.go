package postgres

import (
	"context"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	model2 "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestUpdateValues(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	t.Run("deprecated version", func(t *testing.T) {
		query := Link.
			UPDATE(Link.Name, Link.URL).
			SET("Bong", "http://bong.com").
			WHERE(Link.Name.EQ(String("Bing")))

		testutils.AssertDebugStatementSql(t, query, `
UPDATE test_sample.link
SET (name, url) = ('Bong', 'http://bong.com')
WHERE link.name = 'Bing';
`, "Bong", "http://bong.com", "Bing")

		testutils.AssertExec(t, query, db, 1)
		requireLogged(t, query)

		links := []model.Link{}

		selQuery := Link.
			SELECT(Link.AllColumns).
			WHERE(Link.Name.IN(String("Bong")))

		err := selQuery.Query(db, &links)

		require.NoError(t, err)
		require.Equal(t, len(links), 1)
		testutils.AssertDeepEqual(t, links[0], model.Link{
			ID:   204,
			URL:  "http://bong.com",
			Name: "Bong",
		})
		requireLogged(t, selQuery)
	})

	t.Run("new version", func(t *testing.T) {
		stmt := Link.UPDATE().
			SET(
				Link.Name.SET(String("DuckDuckGo")),
				Link.URL.SET(String("www.duckduckgo.com")),
			).
			WHERE(Link.Name.EQ(String("Yahoo")))

		testutils.AssertDebugStatementSql(t, stmt, `
UPDATE test_sample.link
SET name = 'DuckDuckGo',
    url = 'www.duckduckgo.com'
WHERE link.name = 'Yahoo';
`)
		testutils.AssertExec(t, stmt, db, 1)
		requireLogged(t, stmt)
	})
}

func TestUpdateWithSubQueries(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	t.Run("deprecated version", func(t *testing.T) {
		query := Link.
			UPDATE(Link.Name, Link.URL).
			SET(
				SELECT(String("Bong")),
				SELECT(Link.URL).
					FROM(Link).
					WHERE(Link.Name.EQ(String("Bing"))),
			).
			WHERE(Link.Name.EQ(String("Bing")))

		expectedSQL := `
UPDATE test_sample.link
SET (name, url) = ((
     SELECT 'Bong'
), (
     SELECT link.url AS "link.url"
     FROM test_sample.link
     WHERE link.name = 'Bing'
))
WHERE link.name = 'Bing';
`
		testutils.AssertDebugStatementSql(t, query, expectedSQL, "Bong", "Bing", "Bing")

		AssertExec(t, query, 1)
		requireLogged(t, query)
	})

	t.Run("new version", func(t *testing.T) {
		query := Link.UPDATE().
			SET(
				Link.Name.SET(String("Bong")),
				Link.URL.SET(StringExp(
					SELECT(Link.URL).
						FROM(Link).
						WHERE(Link.Name.EQ(String("Bing")))),
				),
			).
			WHERE(Link.Name.EQ(String("Bing")))

		testutils.AssertStatementSql(t, query, `
UPDATE test_sample.link
SET name = $1,
    url = (
         SELECT link.url AS "link.url"
         FROM test_sample.link
         WHERE link.name = $2
    )
WHERE link.name = $3;
`, "Bong", "Bing", "Bing")
		_, err := query.Exec(db)
		require.NoError(t, err)
		requireLogged(t, query)
	})
}

func TestUpdateAndReturning(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	expectedSQL := `
UPDATE test_sample.link
SET (name, url) = ('DuckDuckGo', 'http://www.duckduckgo.com')
WHERE link.name = 'Ask'
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`

	stmt := Link.
		UPDATE(Link.Name, Link.URL).
		SET("DuckDuckGo", "http://www.duckduckgo.com").
		WHERE(Link.Name.EQ(String("Ask"))).
		RETURNING(Link.AllColumns)

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, "DuckDuckGo", "http://www.duckduckgo.com", "Ask")

	links := []model.Link{}

	err := stmt.Query(db, &links)

	require.NoError(t, err)
	require.Equal(t, len(links), 2)
	require.Equal(t, links[0].Name, "DuckDuckGo")
	require.Equal(t, links[1].Name, "DuckDuckGo")
	requireLogged(t, stmt)
}

func TestUpdateWithSelect(t *testing.T) {

	t.Run("deprecated version", func(t *testing.T) {
		stmt := Link.UPDATE(Link.AllColumns).
			SET(
				Link.
					SELECT(Link.AllColumns).
					WHERE(Link.ID.EQ(Int(0))),
			).
			WHERE(Link.ID.EQ(Int(0)))

		expectedSQL := `
UPDATE test_sample.link
SET (id, url, name, description) = (
     SELECT link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description"
     FROM test_sample.link
     WHERE link.id = 0
)
WHERE link.id = 0;
`
		testutils.AssertDebugStatementSql(t, stmt, expectedSQL, int64(0), int64(0))

		AssertExec(t, stmt, 1)
	})

	t.Run("new version", func(t *testing.T) {
		stmt := Link.UPDATE().
			SET(
				Link.MutableColumns.SET(
					SELECT(Link.MutableColumns).
						FROM(Link).
						WHERE(Link.ID.EQ(Int(0))),
				),
			).
			WHERE(Link.ID.EQ(Int(0)))

		testutils.AssertDebugStatementSql(t, stmt, `
UPDATE test_sample.link
SET (url, name, description) = (
         SELECT link.url AS "link.url",
              link.name AS "link.name",
              link.description AS "link.description"
         FROM test_sample.link
         WHERE link.id = 0
    )
WHERE link.id = 0;
`, int64(0), int64(0))

		AssertExec(t, stmt, 1)
	})
}

func TestUpdateWithInvalidSelect(t *testing.T) {

	t.Run("deprecated version", func(t *testing.T) {
		stmt := Link.UPDATE(Link.AllColumns).
			SET(
				Link.
					SELECT(Link.ID, Link.Name).
					WHERE(Link.ID.EQ(Int(0))),
			).
			WHERE(Link.ID.EQ(Int(0)))

		var expectedSQL = `
UPDATE test_sample.link
SET (id, url, name, description) = (
     SELECT link.id AS "link.id",
          link.name AS "link.name"
     FROM test_sample.link
     WHERE link.id = 0
)
WHERE link.id = 0;
`
		testutils.AssertDebugStatementSql(t, stmt, expectedSQL, int64(0), int64(0))

		testutils.AssertExecErr(t, stmt, db, "pq: number of columns does not match number of values")
	})

	t.Run("new version", func(t *testing.T) {
		stmt := Link.UPDATE().
			SET(Link.AllColumns.SET(Link.SELECT(Link.MutableColumns))).
			WHERE(Link.ID.EQ(Int(0)))

		testutils.AssertExecErr(t, stmt, db, "pq: number of columns does not match number of values")
	})
}

func TestUpdateWithModelData(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	link := model.Link{
		ID:   201,
		URL:  "http://www.duckduckgo.com",
		Name: "DuckDuckGo",
	}

	stmt := Link.
		UPDATE(Link.AllColumns).
		MODEL(link).
		WHERE(Link.ID.EQ(Int32(link.ID)))

	expectedSQL := `
UPDATE test_sample.link
SET (id, url, name, description) = (201, 'http://www.duckduckgo.com', 'DuckDuckGo', NULL)
WHERE link.id = 201::integer;
`
	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, int32(201), "http://www.duckduckgo.com", "DuckDuckGo", nil, int32(201))

	AssertExec(t, stmt, 1)
}

func TestUpdateWithModelDataAndPredefinedColumnList(t *testing.T) {

	setupLinkTableForUpdateTest(t)

	link := model.Link{
		ID:   201,
		URL:  "http://www.duckduckgo.com",
		Name: "DuckDuckGo",
	}

	updateColumnList := ColumnList{Link.Description, Link.Name, Link.URL}

	stmt := Link.
		UPDATE(updateColumnList).
		MODEL(link).
		WHERE(Link.ID.EQ(Int32(link.ID)))

	var expectedSQL = `
UPDATE test_sample.link
SET (description, name, url) = (NULL, 'DuckDuckGo', 'http://www.duckduckgo.com')
WHERE link.id = 201::integer;
`
	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, nil, "DuckDuckGo", "http://www.duckduckgo.com", int32(201))

	AssertExec(t, stmt, 1)
}

func TestUpdateWithInvalidModelData(t *testing.T) {
	defer func() {
		r := recover()

		require.Equal(t, r, "missing struct field for column : id")
	}()

	setupLinkTableForUpdateTest(t)

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

	stmt := Link.
		UPDATE(Link.AllColumns).
		MODEL(link).
		WHERE(Link.ID.EQ(Int(int64(link.Ident))))

	var expectedSQL = `
UPDATE test_sample.link
SET (id, url, name, description, rel) = ('http://www.duckduckgo.com', 'DuckDuckGo', NULL, NULL)
WHERE link.id = 201;
`
	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, "http://www.duckduckgo.com", "DuckDuckGo", nil, nil, int64(201))

	testutils.AssertExecErr(t, stmt, db, "pq: number of columns does not match number of values")
}

func TestUpdateQueryContext(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	updateStmt := Link.
		UPDATE(Link.Name, Link.URL).
		SET("Bong", "http://bong.com").
		WHERE(Link.Name.EQ(String("Bing")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	dest := []model.Link{}
	err := updateStmt.QueryContext(ctx, db, &dest)

	require.Error(t, err, "context deadline exceeded")
}

func TestUpdateExecContext(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	updateStmt := Link.
		UPDATE(Link.Name, Link.URL).
		SET("Bong", "http://bong.com").
		WHERE(Link.Name.EQ(String("Bing")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	_, err := updateStmt.ExecContext(ctx, db)

	require.Error(t, err, "context deadline exceeded")
}

func TestUpdateFrom(t *testing.T) {
	tx := beginTx(t)
	defer tx.Rollback()

	stmt := table.Rental.UPDATE().
		SET(
			table.Rental.RentalDate.SET(Timestamp(2020, 2, 2, 0, 0, 0)),
		).FROM(
		table.Staff.
			INNER_JOIN(table.Store, table.Store.StoreID.EQ(table.Staff.StaffID)),
		table.Actor,
	).WHERE(
		table.Staff.StaffID.EQ(table.Rental.StaffID).
			AND(table.Staff.StaffID.EQ(Int(2))).
			AND(table.Rental.RentalID.LT(Int(10))),
	).RETURNING(
		table.Rental.AllColumns.Except(table.Rental.LastUpdate),
		table.Store.AllColumns.Except(table.Store.LastUpdate),
	)

	testutils.AssertStatementSql(t, stmt, `
UPDATE dvds.rental
SET rental_date = $1::timestamp without time zone
FROM dvds.staff
     INNER JOIN dvds.store ON (store.store_id = staff.staff_id),
     dvds.actor
WHERE ((staff.staff_id = rental.staff_id) AND (staff.staff_id = $2)) AND (rental.rental_id < $3)
RETURNING rental.rental_id AS "rental.rental_id",
          rental.rental_date AS "rental.rental_date",
          rental.inventory_id AS "rental.inventory_id",
          rental.customer_id AS "rental.customer_id",
          rental.return_date AS "rental.return_date",
          rental.staff_id AS "rental.staff_id",
          store.store_id AS "store.store_id",
          store.manager_staff_id AS "store.manager_staff_id",
          store.address_id AS "store.address_id";
`)

	var dest []struct {
		Rental model2.Rental
		Store  model2.Store
	}

	err := stmt.Query(tx, &dest)

	require.NoError(t, err)
	require.Len(t, dest, 3)
	testutils.AssertJSON(t, dest[0], `
{
	"Rental": {
		"RentalID": 4,
		"RentalDate": "2020-02-02T00:00:00Z",
		"InventoryID": 2452,
		"CustomerID": 333,
		"ReturnDate": "2005-06-03T01:43:41Z",
		"StaffID": 2,
		"LastUpdate": "0001-01-01T00:00:00Z"
	},
	"Store": {
		"StoreID": 2,
		"ManagerStaffID": 2,
		"AddressID": 2,
		"LastUpdate": "0001-01-01T00:00:00Z"
	}
}
`)
}

func setupLinkTableForUpdateTest(t *testing.T) {

	cleanUpLinkTable(t)

	_, err := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
		VALUES(200, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		VALUES(201, "http://www.ask.com", "Ask", DEFAULT).
		VALUES(202, "http://www.ask.com", "Ask", DEFAULT).
		VALUES(203, "http://www.yahoo.com", "Yahoo", DEFAULT).
		VALUES(204, "http://www.bing.com", "Bing", DEFAULT).
		Exec(db)

	require.NoError(t, err)
}

func cleanUpLinkTable(t *testing.T) {
	_, err := Link.DELETE().WHERE(Link.ID.GT(Int(0))).Exec(db)
	require.NoError(t, err)
}
