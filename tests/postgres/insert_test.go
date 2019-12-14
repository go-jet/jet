package postgres

import (
	"context"
	"github.com/go-jet/jet/internal/testutils"
	. "github.com/go-jet/jet/postgres"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/table"
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestInsertValues(t *testing.T) {
	cleanUpLinkTable(t)

	var expectedSQL = `
INSERT INTO test_sample.link (id, url, name, description) VALUES
     (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
     (101, 'http://www.google.com', 'Google', DEFAULT),
     (102, 'http://www.yahoo.com', 'Yahoo', NULL)
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`
	insertQuery := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		VALUES(101, "http://www.google.com", "Google", DEFAULT).
		VALUES(102, "http://www.yahoo.com", "Yahoo", nil).
		RETURNING(Link.AllColumns)

	testutils.AssertDebugStatementSql(t, insertQuery, expectedSQL,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		101, "http://www.google.com", "Google",
		102, "http://www.yahoo.com", "Yahoo", nil)

	insertedLinks := []model.Link{}

	err := insertQuery.Query(db, &insertedLinks)

	assert.NilError(t, err)

	assert.Equal(t, len(insertedLinks), 3)

	assert.DeepEqual(t, insertedLinks[0], model.Link{
		ID:   100,
		URL:  "http://www.postgresqltutorial.com",
		Name: "PostgreSQL Tutorial",
	})

	assert.DeepEqual(t, insertedLinks[1], model.Link{
		ID:   101,
		URL:  "http://www.google.com",
		Name: "Google",
	})

	assert.DeepEqual(t, insertedLinks[2], model.Link{
		ID:   102,
		URL:  "http://www.yahoo.com",
		Name: "Yahoo",
	})

	allLinks := []model.Link{}

	err = Link.SELECT(Link.AllColumns).
		WHERE(Link.ID.GT_EQ(Int(100))).
		ORDER_BY(Link.ID).
		Query(db, &allLinks)

	assert.NilError(t, err)

	assert.DeepEqual(t, insertedLinks, allLinks)
}

func TestInsertEmptyColumnList(t *testing.T) {
	cleanUpLinkTable(t)

	expectedSQL := `
INSERT INTO test_sample.link VALUES
     (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT);
`

	stmt := Link.INSERT().
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT)

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial")

	AssertExec(t, stmt, 1)
}

func TestInsertModelObject(t *testing.T) {
	cleanUpLinkTable(t)
	var expectedSQL = `
INSERT INTO test_sample.link (url, name) VALUES
     ('http://www.duckduckgo.com', 'Duck Duck go');
`

	linkData := model.Link{
		URL:  "http://www.duckduckgo.com",
		Name: "Duck Duck go",
	}

	query := Link.
		INSERT(Link.URL, Link.Name).
		MODEL(linkData)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, "http://www.duckduckgo.com", "Duck Duck go")

	AssertExec(t, query, 1)
}

func TestInsertModelObjectEmptyColumnList(t *testing.T) {
	cleanUpLinkTable(t)
	var expectedSQL = `
INSERT INTO test_sample.link VALUES
     (1000, 'http://www.duckduckgo.com', 'Duck Duck go', NULL);
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

	AssertExec(t, query, 1)
}

func TestInsertModelsObject(t *testing.T) {
	expectedSQL := `
INSERT INTO test_sample.link (url, name) VALUES
     ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial'),
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

	stmt := Link.
		INSERT(Link.URL, Link.Name).
		MODELS([]model.Link{tutorial, google, yahoo})

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google",
		"http://www.yahoo.com", "Yahoo")

	AssertExec(t, stmt, 3)
}

func TestInsertUsingMutableColumns(t *testing.T) {
	var expectedSQL = `
INSERT INTO test_sample.link (url, name, description) VALUES
     ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
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

	AssertExec(t, stmt, 4)
}

func TestInsertQuery(t *testing.T) {
	_, err := Link.DELETE().
		WHERE(Link.ID.NOT_EQ(Int(0)).AND(Link.Name.EQ(String("Youtube")))).
		Exec(db)
	assert.NilError(t, err)

	var expectedSQL = `
INSERT INTO test_sample.link (url, name) (
     SELECT link.url AS "link.url",
          link.name AS "link.name"
     FROM test_sample.link
     WHERE link.id = 0
)
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`

	query := Link.
		INSERT(Link.URL, Link.Name).
		QUERY(
			SELECT(Link.URL, Link.Name).
				FROM(Link).
				WHERE(Link.ID.EQ(Int(0))),
		).
		RETURNING(Link.AllColumns)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(0))

	dest := []model.Link{}

	err = query.Query(db, &dest)

	assert.NilError(t, err)

	youtubeLinks := []model.Link{}
	err = Link.
		SELECT(Link.AllColumns).
		WHERE(Link.Name.EQ(String("Youtube"))).
		Query(db, &youtubeLinks)

	assert.NilError(t, err)
	assert.Equal(t, len(youtubeLinks), 2)
}

func TestInsertWithQueryContext(t *testing.T) {
	cleanUpLinkTable(t)

	stmt := Link.INSERT().
		VALUES(1100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		RETURNING(Link.AllColumns)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	dest := []model.Link{}
	err := stmt.QueryContext(ctx, db, &dest)

	assert.Error(t, err, "context deadline exceeded")
}

func TestInsertWithExecContext(t *testing.T) {
	cleanUpLinkTable(t)

	stmt := Link.INSERT().
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	_, err := stmt.ExecContext(ctx, db)

	assert.Error(t, err, "context deadline exceeded")
}
