package mysql

import (
	"context"
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
	cleanUpLinkTable(t)

	var expectedSQL = `
INSERT INTO test_sample.link (id, url, name, description)
VALUES (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
       (101, 'http://www.google.com', 'Google', DEFAULT),
       (102, 'http://www.yahoo.com', 'Yahoo', NULL);
`

	insertQuery := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		VALUES(101, "http://www.google.com", "Google", DEFAULT).
		VALUES(102, "http://www.yahoo.com", "Yahoo", nil)

	testutils.AssertDebugStatementSql(t, insertQuery, expectedSQL,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		101, "http://www.google.com", "Google",
		102, "http://www.yahoo.com", "Yahoo", nil)

	_, err := insertQuery.Exec(db)
	require.NoError(t, err)
	requireLogged(t, insertQuery)

	insertedLinks := []model.Link{}

	err = Link.SELECT(Link.AllColumns).
		WHERE(Link.ID.GT_EQ(Int(100))).
		ORDER_BY(Link.ID).
		Query(db, &insertedLinks)

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
}

var postgreTutorial = model.Link{
	ID:   100,
	URL:  "http://www.postgresqltutorial.com",
	Name: "PostgreSQL Tutorial",
}

func TestInsertEmptyColumnList(t *testing.T) {
	cleanUpLinkTable(t)

	expectedSQL := `
INSERT INTO test_sample.link
VALUES (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT);
`

	stmt := Link.INSERT().
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT)

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial")

	_, err := stmt.Exec(db)
	require.NoError(t, err)
	requireLogged(t, stmt)

	insertedLinks := []model.Link{}

	err = Link.SELECT(Link.AllColumns).
		WHERE(Link.ID.GT_EQ(Int(100))).
		ORDER_BY(Link.ID).
		Query(db, &insertedLinks)

	require.NoError(t, err)
	require.Equal(t, len(insertedLinks), 1)
	testutils.AssertDeepEqual(t, insertedLinks[0], postgreTutorial)
}

func TestInsertModelObject(t *testing.T) {
	cleanUpLinkTable(t)
	var expectedSQL = `
INSERT INTO test_sample.link (url, name)
VALUES ('http://www.duckduckgo.com', 'Duck Duck go');
`

	linkData := model.Link{
		URL:  "http://www.duckduckgo.com",
		Name: "Duck Duck go",
	}

	query := Link.
		INSERT(Link.URL, Link.Name).
		MODEL(linkData)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, "http://www.duckduckgo.com", "Duck Duck go")

	_, err := query.Exec(db)
	require.NoError(t, err)
}

func TestInsertModelObjectEmptyColumnList(t *testing.T) {
	cleanUpLinkTable(t)
	var expectedSQL = `
INSERT INTO test_sample.link
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

	_, err := query.Exec(db)
	require.NoError(t, err)
}

func TestInsertModelsObject(t *testing.T) {
	expectedSQL := `
INSERT INTO test_sample.link (url, name)
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
		MODELS([]model.Link{tutorial, google, yahoo})

	testutils.AssertDebugStatementSql(t, query, expectedSQL,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google",
		"http://www.yahoo.com", "Yahoo")

	_, err := query.Exec(db)
	require.NoError(t, err)
}

func TestInsertUsingMutableColumns(t *testing.T) {
	var expectedSQL = `
INSERT INTO test_sample.link (url, name, description)
VALUES ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
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
		VALUES("http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		MODEL(google).
		MODELS([]model.Link{google, yahoo})

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google", nil,
		"http://www.google.com", "Google", nil,
		"http://www.yahoo.com", "Yahoo", nil)

	_, err := stmt.Exec(db)
	require.NoError(t, err)
}

func TestInsertQuery(t *testing.T) {
	_, err := Link.DELETE().
		WHERE(Link.ID.NOT_EQ(Int(1)).AND(Link.Name.EQ(String("Youtube")))).
		Exec(db)
	require.NoError(t, err)

	var expectedSQL = `
INSERT INTO test_sample.link (url, name) (
     SELECT link.url AS "link.url",
          link.name AS "link.name"
     FROM test_sample.link
     WHERE link.id = 1
);
`

	query := Link.
		INSERT(Link.URL, Link.Name).
		QUERY(
			SELECT(Link.URL, Link.Name).
				FROM(Link).
				WHERE(Link.ID.EQ(Int(1))),
		)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(1))

	_, err = query.Exec(db)
	require.NoError(t, err)

	youtubeLinks := []model.Link{}
	err = Link.
		SELECT(Link.AllColumns).
		WHERE(Link.Name.EQ(String("Youtube"))).
		Query(db, &youtubeLinks)

	require.NoError(t, err)
	require.Equal(t, len(youtubeLinks), 2)
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
ON DUPLICATE KEY UPDATE id = (id + ?),
                        name = ?;
`, randId, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		randId, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		int64(11), "PostgreSQL Tutorial 2")

	testutils.AssertExec(t, stmt, db, 3)

	newLinks := []model.Link{}

	err := SELECT(Link.AllColumns).
		FROM(Link).
		WHERE(Link.ID.EQ(Int32(randId).ADD(Int(11)))).
		Query(db, &newLinks)

	require.NoError(t, err)
	require.Len(t, newLinks, 1)
	require.Equal(t, newLinks[0], model.Link{
		ID:          randId + 11,
		URL:         "http://www.postgresqltutorial.com",
		Name:        "PostgreSQL Tutorial 2",
		Description: nil,
	})
}

func TestInsertWithQueryContext(t *testing.T) {
	cleanUpLinkTable(t)

	stmt := Link.INSERT().
		VALUES(1100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	dest := []model.Link{}
	err := stmt.QueryContext(ctx, db, &dest)

	require.Error(t, err, "context deadline exceeded")
}

func TestInsertWithExecContext(t *testing.T) {
	cleanUpLinkTable(t)

	stmt := Link.INSERT().
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	_, err := stmt.ExecContext(ctx, db)

	require.Error(t, err, "context deadline exceeded")
}

func cleanUpLinkTable(t *testing.T) {
	_, err := Link.DELETE().WHERE(Link.ID.GT(Int(1))).Exec(db)
	require.NoError(t, err)
}
