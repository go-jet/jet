package sqlite

import (
	"context"
	"github.com/go-jet/jet/v2/qrm"
	"database/sql"
	"github.com/go-jet/jet/v2/internal/utils/ptr"
	"math/rand"

	"testing"
	"time"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/table"
	"github.com/stretchr/testify/require"
)

func TestInsertValues(t *testing.T) {
	insertQuery := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil).
		VALUES(101, "http://www.google.com", "Google", "Search engine").
		VALUES(102, "http://www.yahoo.com", "Yahoo", nil)

	testutils.AssertStatementSql(t, insertQuery, `
INSERT INTO link (id, url, name, description)
VALUES (?, ?, ?, ?),
       (?, ?, ?, ?),
       (?, ?, ?, ?);
`, 100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil,
		101, "http://www.google.com", "Google", "Search engine",
		102, "http://www.yahoo.com", "Yahoo", nil)

	testutils.ExecuteInTxAndRollback(t, sampleDB, func(tx qrm.DB) {
		testutils.AssertExec(t, insertQuery, tx)
		requireLogged(t, insertQuery)

		var insertedLinks []model.Link

		err := SELECT(Link.AllColumns).
			FROM(Link).
			WHERE(Link.ID.GT_EQ(Int(100))).
			ORDER_BY(Link.ID).
			Query(tx, &insertedLinks)

		require.NoError(t, err)
		require.Equal(t, len(insertedLinks), 3)
		testutils.AssertDeepEqual(t, insertedLinks[0], postgreTutorial)
		testutils.AssertDeepEqual(t, insertedLinks[1], model.Link{
			ID:          101,
			URL:         "http://www.google.com",
			Name:        "Google",
			Description: ptr.Of("Search engine"),
		})
		testutils.AssertDeepEqual(t, insertedLinks[2], model.Link{
			ID:   102,
			URL:  "http://www.yahoo.com",
			Name: "Yahoo",
		})
	})
}

var postgreTutorial = model.Link{
	ID:   100,
	URL:  "http://www.postgresqltutorial.com",
	Name: "PostgreSQL Tutorial",
}

func TestInsertEmptyColumnList(t *testing.T) {
	stmt := Link.INSERT().
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil)

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO link
VALUES (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', NULL);
`,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil)

	testutils.ExecuteInTxAndRollback(t, sampleDB, func(tx qrm.DB) {
		_, err := stmt.Exec(tx)
		require.NoError(t, err)
		requireLogged(t, stmt)

		var insertedLinks []model.Link

		err = SELECT(Link.AllColumns).
			FROM(Link).
			WHERE(Link.ID.GT_EQ(Int(100))).
			ORDER_BY(Link.ID).
			Query(tx, &insertedLinks)

		require.NoError(t, err)
		require.Equal(t, len(insertedLinks), 1)
		testutils.AssertDeepEqual(t, insertedLinks[0], postgreTutorial)
	})
}

func TestInsertModelObject(t *testing.T) {
	linkData := model.Link{
		URL:  "http://www.duckduckgo.com",
		Name: "Duck Duck go",
	}

	query := Link.INSERT(Link.URL, Link.Name).
		MODEL(linkData)

	testutils.AssertDebugStatementSql(t, query, `
INSERT INTO link (url, name)
VALUES ('http://www.duckduckgo.com', 'Duck Duck go');
`, "http://www.duckduckgo.com", "Duck Duck go")

	testutils.ExecuteInTxAndRollback(t, sampleDB, func(tx qrm.DB) {
		_, err := query.Exec(tx)
		require.NoError(t, err)
	})
}

func TestInsertModelObjectEmptyColumnList(t *testing.T) {
	linkData := model.Link{
		ID:   1000,
		URL:  "http://www.duckduckgo.com",
		Name: "Duck Duck go",
	}

	query := Link.
		INSERT().
		MODEL(linkData)

	testutils.AssertDebugStatementSql(t, query, `
INSERT INTO link
VALUES (1000, 'http://www.duckduckgo.com', 'Duck Duck go', NULL);
`, int32(1000), "http://www.duckduckgo.com", "Duck Duck go", nil)

	testutils.ExecuteInTxAndRollback(t, sampleDB, func(tx qrm.DB) {
		_, err := query.Exec(tx)
		require.NoError(t, err)
	})
}

func TestInsertModelsObject(t *testing.T) {
	tutorial := model.Link{
		URL:  "http://www.postgresqltutorial.com",
		Name: "PostgreSQL Tutorial",
	}
	google := model.Link{
		URL:  "http://www.google.com",
		Name: "Google",
	}
	yahoo := model.Link{
		URL:  "http://www.yahoo.com",
		Name: "Yahoo",
	}

	query := Link.
		INSERT(Link.URL, Link.Name).
		MODELS([]model.Link{
			tutorial,
			google,
			yahoo,
		})

	testutils.AssertDebugStatementSql(t, query, `
INSERT INTO link (url, name)
VALUES ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial'),
       ('http://www.google.com', 'Google'),
       ('http://www.yahoo.com', 'Yahoo');
`,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google",
		"http://www.yahoo.com", "Yahoo")

	testutils.AssertExecAndRollback(t, query, sampleDB)
}

func TestInsertUsingMutableColumns(t *testing.T) {
	google := model.Link{
		URL:  "http://www.google.com",
		Name: "Google",
	}

	yahoo := model.Link{
		URL:  "http://www.yahoo.com",
		Name: "Yahoo",
	}

	stmt := Link.
		INSERT(Link.MutableColumns).
		VALUES("http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil).
		MODEL(google).
		MODELS([]model.Link{google, yahoo})

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO link (url, name, description)
VALUES ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', NULL),
       ('http://www.google.com', 'Google', NULL),
       ('http://www.google.com', 'Google', NULL),
       ('http://www.yahoo.com', 'Yahoo', NULL);
`,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil,
		"http://www.google.com", "Google", nil,
		"http://www.google.com", "Google", nil,
		"http://www.yahoo.com", "Yahoo", nil)

	testutils.AssertExecAndRollback(t, stmt, sampleDB)
}

func TestInsertQuery(t *testing.T) {
	var expectedSQL = `
INSERT INTO link (url, name)
SELECT link.url AS "link.url",
     link.name AS "link.name"
FROM link
WHERE link.id = 24;
`
	query := Link.INSERT(Link.URL, Link.Name).
		QUERY(
			SELECT(Link.URL, Link.Name).
				FROM(Link).
				WHERE(Link.ID.EQ(Int(24))),
		)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(24))
	testutils.ExecuteInTxAndRollback(t, sampleDB, func(tx qrm.DB) {
		_, err := query.Exec(tx)
		require.NoError(t, err)

		var youtubeLinks []model.Link
		err = Link.
			SELECT(Link.AllColumns).
			WHERE(Link.Name.EQ(String("Bing"))).
			Query(tx, &youtubeLinks)

		require.NoError(t, err)
		require.Equal(t, len(youtubeLinks), 2)
	})
}

func TestInsert_DEFAULT_VALUES_RETURNING(t *testing.T) {
	stmt := Link.INSERT().
		DEFAULT_VALUES().
		RETURNING(Link.AllColumns)

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO link
DEFAULT VALUES
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`)

	testutils.ExecuteInTxAndRollback(t, sampleDB, func(tx qrm.DB) {
		var link model.Link
		err := stmt.Query(tx, &link)
		require.NoError(t, err)

		require.EqualValues(t, link, model.Link{
			ID:          25,
			URL:         "www.",
			Name:        "_",
			Description: nil,
		})
	})
}

func TestInsertOnConflict(t *testing.T) {

	t.Run("do nothing", func(t *testing.T) {
		link := model.Link{ID: rand.Int31()}

		stmt := Link.INSERT(Link.AllColumns).
			MODEL(link).
			MODEL(link).
			ON_CONFLICT().DO_NOTHING()

		testutils.AssertStatementSql(t, stmt, `
INSERT INTO link (id, url, name, description)
VALUES (?, ?, ?, ?),
       (?, ?, ?, ?)
ON CONFLICT DO NOTHING;
`)
		testutils.AssertExecAndRollback(t, stmt, sampleDB, 1)
		requireLogged(t, stmt)
	})

	t.Run("do nothing with index", func(t *testing.T) {
		link := model.Link{ID: rand.Int31()}

		stmt := Link.INSERT(Link.AllColumns).
			MODEL(link).
			MODEL(link).
			ON_CONFLICT(Link.ID).DO_NOTHING()

		testutils.AssertStatementSql(t, stmt, `
INSERT INTO link (id, url, name, description)
VALUES (?, ?, ?, ?),
       (?, ?, ?, ?)
ON CONFLICT (id) DO NOTHING;
`)
		testutils.AssertExecAndRollback(t, stmt, sampleDB, 1)
		requireLogged(t, stmt)
	})

	t.Run("do update", func(t *testing.T) {
		stmt := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
			VALUES(21, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil).
			VALUES(22, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil).
			ON_CONFLICT(Link.ID).
			DO_UPDATE(
				SET(
					Link.ID.SET(Link.EXCLUDED.ID),
					Link.URL.SET(String("http://www.postgresqltutorial2.com")),
				),
			).RETURNING(Link.AllColumns)

		testutils.AssertStatementSql(t, stmt, `
INSERT INTO link (id, url, name, description)
VALUES (?, ?, ?, ?),
       (?, ?, ?, ?)
ON CONFLICT (id) DO UPDATE
       SET id = excluded.id,
           url = ?
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`)

		testutils.AssertExecAndRollback(t, stmt, sampleDB)
		requireLogged(t, stmt)
	})

	t.Run("do update complex", func(t *testing.T) {
		stmt := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
			VALUES(21, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil).
			ON_CONFLICT(Link.ID).
			WHERE(Link.ID.MUL(Int(2)).GT(Int(10))).
			DO_UPDATE(
				SET(
					Link.ID.SET(
						IntExp(SELECT(MAXi(Link.ID).ADD(Int(1))).
							FROM(Link)),
					),
					ColumnList{Link.Name, Link.Description}.SET(ROW(Link.EXCLUDED.Name, String(""))),
				).WHERE(Link.Description.IS_NOT_NULL()),
			)

		testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO link (id, url, name, description)
VALUES (21, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', NULL)
ON CONFLICT (id) WHERE (id * 2) > 10 DO UPDATE
       SET id = (
                SELECT MAX(link.id) + 1
                FROM link
           ),
           (name, description) = (excluded.name, '')
       WHERE link.description IS NOT NULL;
`)

		testutils.AssertExecAndRollback(t, stmt, sampleDB)
		requireLogged(t, stmt)
	})

	t.Run("nil action removes ON CONFLICT clause", func(t *testing.T) {
		link := model.Link{ID: rand.Int31()}

		stmt := Link.INSERT(Link.AllColumns).
			MODEL(link).
			ON_CONFLICT().DO_UPDATE(nil)

		testutils.AssertStatementSql(t, stmt, `
INSERT INTO link (id, url, name, description)
VALUES (?, ?, ?, ?);
`)
		testutils.AssertExecAndRollback(t, stmt, sampleDB, 1)
		requireLogged(t, stmt)
	})
}

func TestInsertContextDeadlineExceeded(t *testing.T) {
	stmt := Link.INSERT().
		VALUES(1100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	var dest []model.Link

	testutils.ExecuteInTxAndRollback(t, sampleDB, func(tx qrm.DB) {
		err := stmt.QueryContext(ctx, tx, &dest)
		require.Error(t, err, "context deadline exceeded")
	})

	testutils.ExecuteInTxAndRollback(t, sampleDB, func(tx qrm.DB) {
		_, err := stmt.ExecContext(ctx, tx)
		require.Error(t, err, "context deadline exceeded")
	})

}
