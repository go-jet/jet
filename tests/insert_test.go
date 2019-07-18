package tests

import (
	. "github.com/go-jet/jet"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/table"
	"gotest.tools/assert"
	"testing"
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

	assertStatementSql(t, insertQuery, expectedSQL,
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

	assertStatementSql(t, stmt, expectedSQL,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial")

	assertExec(t, stmt, 1)
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

	assertStatementSql(t, query, expectedSQL, "http://www.duckduckgo.com", "Duck Duck go")

	result, err := query.Exec(db)

	assert.NilError(t, err)

	rowsAffected, err := result.RowsAffected()

	assert.Equal(t, rowsAffected, int64(1))
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

	assertStatementSql(t, stmt, expectedSQL,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google",
		"http://www.yahoo.com", "Yahoo")

	assertExec(t, stmt, 3)
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

	assertStatementSql(t, stmt, expectedSQL,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google", nil,
		"http://www.google.com", "Google", nil,
		"http://www.yahoo.com", "Yahoo", nil)

	assertExec(t, stmt, 4)
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

	assertStatementSql(t, query, expectedSQL, int64(0))

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
