package sqlite

import (
	"context"
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
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

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

	_, err := insertQuery.Exec(tx)
	require.NoError(t, err)
	requireLogged(t, insertQuery)

	insertedLinks := []model.Link{}

	err = SELECT(Link.AllColumns).
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
		Description: testutils.StringPtr("Search engine"),
	})
	testutils.AssertDeepEqual(t, insertedLinks[2], model.Link{
		ID:   102,
		URL:  "http://www.yahoo.com",
		Name: "Yahoo",
	})
}

var postgreTutorial = model.Link{
	ID:   100,
	URL:  "http://www.postgresqltutorial.com",
	Name: "PostgreSQL Tutorial",
}

func TestInsertEmptyColumnList(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	expectedSQL := `
INSERT INTO link
VALUES (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', NULL);
`

	stmt := Link.INSERT().
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil)

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil)

	_, err := stmt.Exec(tx)
	require.NoError(t, err)
	requireLogged(t, stmt)

	insertedLinks := []model.Link{}

	err = SELECT(Link.AllColumns).
		FROM(Link).
		WHERE(Link.ID.GT_EQ(Int(100))).
		ORDER_BY(Link.ID).
		Query(tx, &insertedLinks)

	require.NoError(t, err)
	require.Equal(t, len(insertedLinks), 1)
	testutils.AssertDeepEqual(t, insertedLinks[0], postgreTutorial)
}

func TestInsertModelObject(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

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

	_, err := query.Exec(tx)
	require.NoError(t, err)
}

func TestInsertModelObjectEmptyColumnList(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	var expectedSQL = `
INSERT INTO link
VALUES (1000, 'http://www.duckduckgo.com', 'Duck Duck go', NULL);
`

	linkData := model.Link{
		ID:   1000,
		URL:  "http://www.duckduckgo.com",
		Name: "Duck Duck go",
	}

	query := Link.
		INSERT().
		MODEL(linkData)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int32(1000), "http://www.duckduckgo.com", "Duck Duck go", nil)

	_, err := query.Exec(tx)
	require.NoError(t, err)
}

func TestInsertModelsObject(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	expectedSQL := `
INSERT INTO link (url, name)
VALUES ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial'),
       ('http://www.google.com', 'Google'),
       ('http://www.yahoo.com', 'Yahoo');
`

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

	testutils.AssertDebugStatementSql(t, query, expectedSQL,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google",
		"http://www.yahoo.com", "Yahoo")

	_, err := query.Exec(tx)
	require.NoError(t, err)
}

func TestInsertUsingMutableColumns(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	var expectedSQL = `
INSERT INTO link (url, name, description)
VALUES ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', NULL),
       ('http://www.google.com', 'Google', NULL),
       ('http://www.google.com', 'Google', NULL),
       ('http://www.yahoo.com', 'Yahoo', NULL);
`

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

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil,
		"http://www.google.com", "Google", nil,
		"http://www.google.com", "Google", nil,
		"http://www.yahoo.com", "Yahoo", nil)

	_, err := stmt.Exec(tx)
	require.NoError(t, err)
}

func TestInsertQuery(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

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

	_, err := query.Exec(tx)
	require.NoError(t, err)

	youtubeLinks := []model.Link{}
	err = Link.
		SELECT(Link.AllColumns).
		WHERE(Link.Name.EQ(String("Bing"))).
		Query(tx, &youtubeLinks)

	require.NoError(t, err)
	require.Equal(t, len(youtubeLinks), 2)
}

func TestInsert_DEFAULT_VALUES_RETURNING(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

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

	var link model.Link
	err := stmt.Query(tx, &link)
	require.NoError(t, err)

	require.EqualValues(t, link, model.Link{
		ID:          25,
		URL:         "www.",
		Name:        "_",
		Description: nil,
	})
}

func TestInsertOnConflict(t *testing.T) {

	t.Run("do nothing", func(t *testing.T) {
		tx := beginSampleDBTx(t)
		defer tx.Rollback()

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
		testutils.AssertExec(t, stmt, tx, 1)
		requireLogged(t, stmt)
	})

	t.Run("do update", func(t *testing.T) {
		tx := beginSampleDBTx(t)
		defer tx.Rollback()

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

		testutils.AssertExec(t, stmt, tx)
		requireLogged(t, stmt)
	})

	t.Run("do update complex", func(t *testing.T) {
		tx := beginSampleDBTx(t)
		defer tx.Rollback()

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

		testutils.AssertExec(t, stmt, tx)
		requireLogged(t, stmt)
	})
}

func TestInsertContextDeadlineExceeded(t *testing.T) {
	stmt := Link.INSERT().
		VALUES(1100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	dest := []model.Link{}
	err := stmt.QueryContext(ctx, sampleDB, &dest)
	require.Error(t, err, "context deadline exceeded")

	_, err = stmt.ExecContext(ctx, db)
	require.Error(t, err, "context deadline exceeded")
}
