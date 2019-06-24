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

	var expectedSql = `
INSERT INTO test_sample.link (id, url, name, rel) VALUES
     (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
     (101, 'http://www.google.com', 'Google', DEFAULT),
     (102, 'http://www.yahoo.com', 'Yahoo', NULL)
RETURNING link.id AS "link.id",
     link.url AS "link.url",
     link.name AS "link.name",
     link.description AS "link.description",
     link.rel AS "link.rel";
`

	insertQuery := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Rel).
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		VALUES(101, "http://www.google.com", "Google", DEFAULT).
		VALUES(102, "http://www.yahoo.com", "Yahoo", nil).
		RETURNING(Link.AllColumns)

	assertStatementSql(t, insertQuery, expectedSql,
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
		Rel:  nil,
	})

	assert.DeepEqual(t, insertedLinks[1], model.Link{
		ID:   101,
		URL:  "http://www.google.com",
		Name: "Google",
		Rel:  nil,
	})

	assert.DeepEqual(t, insertedLinks[2], model.Link{
		ID:   102,
		URL:  "http://www.yahoo.com",
		Name: "Yahoo",
		Rel:  nil,
	})

	allLinks := []model.Link{}

	err = Link.SELECT(Link.AllColumns).
		WHERE(Link.ID.GT_EQ(Int(100))).
		ORDER_BY(Link.ID).
		Query(db, &allLinks)

	assert.NilError(t, err)

	assert.DeepEqual(t, insertedLinks, allLinks)
}

func TestInsertDataObject(t *testing.T) {
	var expectedSql = `
INSERT INTO test_sample.link (url, name) VALUES
     ('http://www.duckduckgo.com', 'Duck Duck go');
`

	linkData := model.Link{
		URL:  "http://www.duckduckgo.com",
		Name: "Duck Duck go",
		Rel:  nil,
	}

	query := Link.
		INSERT(Link.URL, Link.Name).
		USING(linkData)

	assertStatementSql(t, query, expectedSql, "http://www.duckduckgo.com", "Duck Duck go")

	result, err := query.Exec(db)

	assert.NilError(t, err)

	rowsAffected, err := result.RowsAffected()

	assert.Equal(t, rowsAffected, int64(1))
}

func TestInsertQuery(t *testing.T) {
	_, err := Link.DELETE().
		WHERE(Link.ID.NOT_EQ(Int(0)).AND(Link.Name.EQ(String("Youtube")))).
		Exec(db)
	assert.NilError(t, err)

	var expectedSql = `
INSERT INTO test_sample.link (url, name) (
     SELECT link.url AS "link.url",
          link.name AS "link.name"
     FROM test_sample.link
     WHERE link.id = 0
)
RETURNING link.id AS "link.id",
     link.url AS "link.url",
     link.name AS "link.name",
     link.description AS "link.description",
     link.rel AS "link.rel";
`

	query := Link.
		INSERT(Link.URL, Link.Name).
		QUERY(
			SELECT(Link.URL, Link.Name).
				FROM(Link).
				WHERE(Link.ID.EQ(Int(0))),
		).
		RETURNING(Link.AllColumns)

	assertStatementSql(t, query, expectedSql, int64(0))

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
