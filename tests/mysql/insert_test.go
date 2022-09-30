package mysql

import (
	"context"
	"database/sql"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/table"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestInsertValues(t *testing.T) {
	insertQuery := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		VALUES(101, "http://www.google.com", "Google", DEFAULT).
		VALUES(102, "http://www.yahoo.com", "Yahoo", nil)

	testutils.AssertDebugStatementSql(t, insertQuery, `
INSERT INTO test_sample.link (id, url, name, description)
VALUES (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
       (101, 'http://www.google.com', 'Google', DEFAULT),
       (102, 'http://www.yahoo.com', 'Yahoo', NULL);
`,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		101, "http://www.google.com", "Google",
		102, "http://www.yahoo.com", "Yahoo", nil)

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := insertQuery.Exec(tx)
		require.NoError(t, err)
		requireLogged(t, insertQuery)

		var insertedLinks []model.Link

		err = Link.SELECT(Link.AllColumns).
			WHERE(Link.ID.BETWEEN(Int(100), Int(199))).
			ORDER_BY(Link.ID).
			Query(tx, &insertedLinks)

		require.NoError(t, err)
		require.Equal(t, len(insertedLinks), 3)

		testutils.AssertDeepEqual(t, insertedLinks[0], postgreTutorial)
		testutils.AssertDeepEqual(t, insertedLinks[1], model.Link{
			ID:   101,
			URL:  "http://www.google.com",
			Name: "Google",
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
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT)

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO test_sample.link
VALUES (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT);
`,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial")

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := stmt.Exec(tx)
		require.NoError(t, err)
		requireLogged(t, stmt)

		var insertedLinks []model.Link

		err = Link.SELECT(Link.AllColumns).
			WHERE(Link.ID.BETWEEN(Int(100), Int(199))).
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

	query := Link.
		INSERT(Link.URL, Link.Name).
		MODEL(linkData)

	testutils.AssertDebugStatementSql(t, query, `
INSERT INTO test_sample.link (url, name)
VALUES ('http://www.duckduckgo.com', 'Duck Duck go');
`,
		"http://www.duckduckgo.com", "Duck Duck go")

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
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
INSERT INTO test_sample.link
VALUES (1000, 'http://www.duckduckgo.com', 'Duck Duck go', NULL);
`, int32(1000), "http://www.duckduckgo.com", "Duck Duck go", nil)

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
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
		MODELS([]model.Link{tutorial, google, yahoo})

	testutils.AssertDebugStatementSql(t, query, `
INSERT INTO test_sample.link (url, name)
VALUES ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial'),
       ('http://www.google.com', 'Google'),
       ('http://www.yahoo.com', 'Yahoo');
`,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google",
		"http://www.yahoo.com", "Yahoo")

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := query.Exec(tx)
		require.NoError(t, err)
	})
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
		VALUES("http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		MODEL(google).
		MODELS([]model.Link{google, yahoo})

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO test_sample.link (url, name, description)
VALUES ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
       ('http://www.google.com', 'Google', NULL),
       ('http://www.google.com', 'Google', NULL),
       ('http://www.yahoo.com', 'Yahoo', NULL);
`,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google", nil,
		"http://www.google.com", "Google", nil,
		"http://www.yahoo.com", "Yahoo", nil)

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := stmt.Exec(tx)
		require.NoError(t, err)
	})
}

func TestInsertQuery(t *testing.T) {
	query := Link.
		INSERT(Link.URL, Link.Name).
		QUERY(
			SELECT(Link.URL, Link.Name).
				FROM(Link).
				WHERE(Link.ID.EQ(Int(1))),
		)

	testutils.AssertDebugStatementSql(t, query, `
INSERT INTO test_sample.link (url, name) (
     SELECT link.url AS "link.url",
          link.name AS "link.name"
     FROM test_sample.link
     WHERE link.id = 1
);
`, int64(1))

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := query.Exec(tx)
		require.NoError(t, err)

		var youtubeLinks []model.Link
		err = Link.
			SELECT(Link.AllColumns).
			WHERE(Link.Name.EQ(String("Youtube"))).
			Query(tx, &youtubeLinks)

		require.NoError(t, err)
		require.Equal(t, len(youtubeLinks), 2)
	})
}

func TestInsertOnDuplicateKey(t *testing.T) {
	randId := rand.Int31()

	stmt := Link.INSERT().
		VALUES(randId, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		VALUES(randId, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		ON_DUPLICATE_KEY_UPDATE(
			Link.ID.SET(Link.ID.ADD(Int(11))),
			Link.Name.SET(String("PostgreSQL Tutorial 2")),
		)

	testutils.AssertStatementSql(t, stmt, `
INSERT INTO test_sample.link
VALUES (?, ?, ?, DEFAULT),
       (?, ?, ?, DEFAULT)
ON DUPLICATE KEY UPDATE id = (link.id + ?),
                        name = ?;
`, randId, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		randId, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		int64(11), "PostgreSQL Tutorial 2")

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := stmt.Exec(tx)
		require.NoError(t, err)

		var newLinks []model.Link

		err = SELECT(Link.AllColumns).
			FROM(Link).
			WHERE(Link.ID.EQ(Int32(randId).ADD(Int(11)))).
			Query(tx, &newLinks)

		require.NoError(t, err)
		require.Len(t, newLinks, 1)
		require.Equal(t, newLinks[0], model.Link{
			ID:          randId + 11,
			URL:         "http://www.postgresqltutorial.com",
			Name:        "PostgreSQL Tutorial 2",
			Description: nil,
		})
	})
}

func TestInsertOnDuplicateKeyUpdateNEW(t *testing.T) {
	skipForMariaDB(t)

	randId := rand.Int31()

	stmt := Link.INSERT().
		MODELS([]model.Link{
			{
				ID:          randId,
				URL:         "https://www.postgresqltutorial.com",
				Name:        "PostgreSQL Tutorial",
				Description: nil,
			},
			{
				ID:          randId,
				URL:         "https://www.yahoo.com",
				Name:        "Yahoo",
				Description: testutils.StringPtr("web portal and search engine"),
			},
		}).AS_NEW().
		ON_DUPLICATE_KEY_UPDATE(
			Link.ID.SET(Link.ID.ADD(Int(11))),
			Link.URL.SET(Link.NEW.URL),
			Link.Name.SET(Link.NEW.Name),
			Link.Description.SET(Link.NEW.Description),
		)

	testutils.AssertStatementSql(t, stmt, `
INSERT INTO test_sample.link
VALUES (?, ?, ?, ?),
       (?, ?, ?, ?) AS new
ON DUPLICATE KEY UPDATE id = (link.id + ?),
                        url = new.url,
                        name = new.name,
                        description = new.description;
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := stmt.Exec(tx)
		require.NoError(t, err)

		stmt := SELECT(Link.AllColumns).
			FROM(Link).
			WHERE(Link.ID.EQ(Int32(randId + 11)))

		var dest model.Link

		err = stmt.Query(tx, &dest)
		require.NoError(t, err)

		testutils.AssertDeepEqual(t, dest, model.Link{
			ID:          randId + 11,
			URL:         "https://www.yahoo.com",
			Name:        "Yahoo",
			Description: testutils.StringPtr("web portal and search engine"),
		})
	})
}

func TestInsertWithQueryContext(t *testing.T) {
	stmt := Link.INSERT().
		VALUES(1100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	var dest []model.Link
	err := stmt.QueryContext(ctx, db, &dest)

	require.Error(t, err, "context deadline exceeded")
}

func TestInsertWithExecContext(t *testing.T) {
	stmt := Link.INSERT().
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	_, err := stmt.ExecContext(ctx, db)

	require.Error(t, err, "context deadline exceeded")
}

func TestInsertOptimizerHints(t *testing.T) {

	stmt := Link.INSERT(Link.MutableColumns).
		OPTIMIZER_HINTS(QB_NAME("qbIns"), "NO_ICP(link)").
		MODEL(model.Link{
			URL:  "http://www.google.com",
			Name: "Google",
		})

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT /*+ QB_NAME(qbIns) NO_ICP(link) */ INTO test_sample.link (url, name, description)
VALUES ('http://www.google.com', 'Google', NULL);
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := stmt.Exec(tx)
		require.NoError(t, err)
	})
}
