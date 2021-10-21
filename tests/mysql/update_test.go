package mysql

import (
	"context"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/table"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestUpdateValues(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	var expectedSQL = `
UPDATE test_sample.link
SET name = 'Bong',
    url = 'http://bong.com'
WHERE link.name = 'Bing';
`
	t.Run("old version", func(t *testing.T) {
		query := Link.
			UPDATE(Link.Name, Link.URL).
			SET("Bong", "http://bong.com").
			WHERE(Link.Name.EQ(String("Bing")))

		testutils.AssertDebugStatementSql(t, query, expectedSQL, "Bong", "http://bong.com", "Bing")
		testutils.AssertExec(t, query, db)
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
		testutils.AssertExec(t, stmt, db)
		requireLogged(t, stmt)
	})

	links := []model.Link{}

	err := Link.
		SELECT(Link.AllColumns).
		WHERE(Link.Name.EQ(String("Bong"))).
		Query(db, &links)

	require.NoError(t, err)
	require.Equal(t, len(links), 1)
	testutils.AssertDeepEqual(t, links[0], model.Link{
		ID:   204,
		URL:  "http://bong.com",
		Name: "Bong",
	})
}

func TestUpdateWithSubQueries(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	expectedSQL := `
UPDATE test_sample.link
SET name = ?,
    url = (
         SELECT link2.url AS "link2.url"
         FROM test_sample.link2
         WHERE link2.name = ?
    )
WHERE link.name = ?;
`
	t.Run("old version", func(t *testing.T) {
		query := Link.
			UPDATE(Link.Name, Link.URL).
			SET(
				String("Bong"),
				SELECT(Link2.URL).
					FROM(Link2).
					WHERE(Link2.Name.EQ(String("Youtube"))),
			).
			WHERE(Link.Name.EQ(String("Bing")))

		testutils.AssertStatementSql(t, query, expectedSQL, "Bong", "Youtube", "Bing")
		testutils.AssertExec(t, query, db)
		requireLogged(t, query)
	})

	t.Run("new version", func(t *testing.T) {
		query := Link.
			UPDATE().
			SET(
				Link.Name.SET(String("Bong")),
				Link.URL.SET(StringExp(
					SELECT(Link2.URL).
						FROM(Link2).
						WHERE(Link2.Name.EQ(String("Youtube"))),
				)),
			).
			WHERE(Link.Name.EQ(String("Bing")))

		testutils.AssertStatementSql(t, query, expectedSQL, "Bong", "Youtube", "Bing")
		testutils.AssertExec(t, query, db)
		requireLogged(t, query)
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
SET id = ?,
    url = ?,
    name = ?,
    description = ?
WHERE link.id = ?;
`
	testutils.AssertStatementSql(t, stmt, expectedSQL, int32(201), "http://www.duckduckgo.com", "DuckDuckGo", nil, int32(201))

	testutils.AssertExec(t, stmt, db)
	requireLogged(t, stmt)
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
SET description = NULL,
    name = 'DuckDuckGo',
    url = 'http://www.duckduckgo.com'
WHERE link.id = 201;
`

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, nil, "DuckDuckGo", "http://www.duckduckgo.com", int32(201))

	testutils.AssertExec(t, stmt, db)
	requireLogged(t, stmt)
}

func TestUpdateWithModelDataAndMutableColumns(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	link := model.Link{
		ID:   201,
		URL:  "http://www.duckduckgo.com",
		Name: "DuckDuckGo",
	}

	stmt := Link.
		UPDATE(Link.MutableColumns).
		MODEL(link).
		WHERE(Link.ID.EQ(Int32(link.ID)))

	var expectedSQL = `
UPDATE test_sample.link
SET url = 'http://www.duckduckgo.com',
    name = 'DuckDuckGo',
    description = NULL
WHERE link.id = 201;
`
	//fmt.Println(stmt.DebugSql())

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, "http://www.duckduckgo.com", "DuckDuckGo", nil, int32(201))
	testutils.AssertExec(t, stmt, db)
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

	stmt.Sql()
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

func TestUpdateWithJoin(t *testing.T) {
	query := table.Staff.
		INNER_JOIN(table.Address, table.Address.AddressID.EQ(table.Staff.AddressID)).
		UPDATE(table.Staff.LastName).
		SET(String("New name")).
		WHERE(table.Staff.StaffID.EQ(Int(1)))

	//fmt.Println(query.DebugSql())

	_, err := query.Exec(db)
	require.NoError(t, err)
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
