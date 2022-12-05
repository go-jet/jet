package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/table"
)

func TestUpdateValues(t *testing.T) {
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
		testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
			testutils.AssertExec(t, query, tx)
			requireLogged(t, query)

			var links []model.Link

			err := Link.
				SELECT(Link.AllColumns).
				WHERE(Link.Name.EQ(String("Bong"))).
				Query(tx, &links)

			require.NoError(t, err)
			require.Equal(t, len(links), 1)
			testutils.AssertDeepEqual(t, links[0], model.Link{
				ID:   204,
				URL:  "http://bong.com",
				Name: "Bong",
			})
		})

	})

	t.Run("new version", func(t *testing.T) {
		stmt := Link.UPDATE().
			SET(
				Link.Name.SET(String("Bong")),
				Link.URL.SET(String("http://bong.com")),
			).
			WHERE(Link.Name.EQ(String("Bing")))

		testutils.AssertDebugStatementSql(t, stmt, expectedSQL, "Bong", "http://bong.com", "Bing")
		testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
			testutils.AssertExec(t, stmt, tx)
			requireLogged(t, stmt)

			var links []model.Link

			err := Link.
				SELECT(Link.AllColumns).
				WHERE(Link.Name.EQ(String("Bong"))).
				Query(tx, &links)

			require.NoError(t, err)
			require.Equal(t, len(links), 1)
			testutils.AssertDeepEqual(t, links[0], model.Link{
				ID:   204,
				URL:  "http://bong.com",
				Name: "Bong",
			})
		})
	})
}

func TestUpdateWithSubQueries(t *testing.T) {
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
		testutils.AssertExecAndRollback(t, query, db)
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
		testutils.AssertExecAndRollback(t, query, db)
		requireLogged(t, query)
	})
}

func TestUpdateWithModelData(t *testing.T) {
	link := model.Link{
		ID:   201,
		URL:  "http://www.duckduckgo.com",
		Name: "DuckDuckGo",
	}

	stmt := Link.
		UPDATE(Link.AllColumns).
		MODEL(link).
		WHERE(Link.ID.EQ(Int32(link.ID)))

	testutils.AssertStatementSql(t, stmt, `
UPDATE test_sample.link
SET id = ?,
    url = ?,
    name = ?,
    description = ?
WHERE link.id = ?;
`, int32(201), "http://www.duckduckgo.com", "DuckDuckGo", nil, int32(201))

	testutils.AssertExecAndRollback(t, stmt, db)
	requireLogged(t, stmt)
}

func TestUpdateWithModelDataAndPredefinedColumnList(t *testing.T) {
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

	testutils.AssertDebugStatementSql(t, stmt, `
UPDATE test_sample.link
SET description = NULL,
    name = 'DuckDuckGo',
    url = 'http://www.duckduckgo.com'
WHERE link.id = 201;
`, nil, "DuckDuckGo", "http://www.duckduckgo.com", int32(201))

	testutils.AssertExecAndRollback(t, stmt, db)
	requireLogged(t, stmt)
}

func TestUpdateWithModelDataAndMutableColumns(t *testing.T) {
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
	testutils.AssertExecAndRollback(t, stmt, db)
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

	_ = Link.
		UPDATE(Link.AllColumns).
		MODEL(link).
		WHERE(Link.ID.EQ(Int(int64(link.Ident))))
}

func TestUpdateQueryContext(t *testing.T) {
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

func TestUpdateOptimizerHints(t *testing.T) {

	stmt := Link.UPDATE(Link.AllColumns).
		OPTIMIZER_HINTS(QB_NAME("qbInsert"), "MRR(link)").
		MODEL(model.Link{
			ID:   501,
			URL:  "http://www.duckduckgo.com",
			Name: "DuckDuckGo",
		}).
		WHERE(Link.Name.EQ(String("Bing")))

	testutils.AssertDebugStatementSql(t, stmt, `
UPDATE /*+ QB_NAME(qbInsert) MRR(link) */ test_sample.link
SET id = 501,
    url = 'http://www.duckduckgo.com',
    name = 'DuckDuckGo',
    description = NULL
WHERE link.name = 'Bing';
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := stmt.Exec(tx)
		require.NoError(t, err)
	})
}
