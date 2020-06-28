package postgres

import (
	"context"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
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
		WHERE(Link.ID.EQ(Int(int64(link.ID))))

	expectedSQL := `
UPDATE test_sample.link
SET (id, url, name, description) = (201, 'http://www.duckduckgo.com', 'DuckDuckGo', NULL)
WHERE link.id = 201;
`
	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, int32(201), "http://www.duckduckgo.com", "DuckDuckGo", nil, int64(201))

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
		WHERE(Link.ID.EQ(Int(int64(link.ID))))

	var expectedSQL = `
UPDATE test_sample.link
SET (description, name, url) = (NULL, 'DuckDuckGo', 'http://www.duckduckgo.com')
WHERE link.id = 201;
`
	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, nil, "DuckDuckGo", "http://www.duckduckgo.com", int64(201))

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
