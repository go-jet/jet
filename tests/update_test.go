package tests

import (
	"fmt"
	. "github.com/go-jet/jet/sqlbuilder"
	"github.com/go-jet/jet/tests/.test_files/dvd_rental/test_sample/model"
	. "github.com/go-jet/jet/tests/.test_files/dvd_rental/test_sample/table"
	"gotest.tools/assert"
	"testing"
)

func TestUpdateValues(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	query := Link.
		UPDATE(Link.Name, Link.URL).
		SET("Bong", "http://bong.com").
		WHERE(Link.Name.EQ(String("Bing")))

	fmt.Println(query.DebugSql())

	var expectedSql = `
UPDATE test_sample.link
SET (name, url) = ('Bong', 'http://bong.com')
WHERE link.name = 'Bing';
`
	fmt.Println(query.Sql())

	assertStatementSql(t, query, expectedSql, "Bong", "http://bong.com", "Bing")

	assertExec(t, query, 1)

	links := []model.Link{}

	err := Link.
		SELECT(Link.AllColumns).
		WHERE(Link.Name.EQ(String("Bong"))).
		Query(db, &links)

	assert.NilError(t, err)
	assert.Equal(t, len(links), 1)
	assert.DeepEqual(t, links[0], model.Link{
		ID:   204,
		URL:  "http://bong.com",
		Name: "Bong",
	})
}

func TestUpdateWithSubQueries(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	query := Link.
		UPDATE(Link.Name, Link.URL).
		SET(
			SELECT(String("Bong")),
			SELECT(Link.URL).
				FROM(Link).
				WHERE(Link.Name.EQ(String("Bing"))),
		).
		WHERE(Link.Name.EQ(String("Bing")))

	expectedSql := `
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

	assertStatementSql(t, query, expectedSql, "Bong", "Bing", "Bing")

	assertExec(t, query, 1)
}

func TestUpdateAndReturning(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	expectedSql := `
UPDATE test_sample.link
SET (name, url) = ('DuckDuckGo', 'http://www.duckduckgo.com')
WHERE link.name = 'Ask'
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description",
          link.rel AS "link.rel";
`

	stmt := Link.
		UPDATE(Link.Name, Link.URL).
		SET("DuckDuckGo", "http://www.duckduckgo.com").
		WHERE(Link.Name.EQ(String("Ask"))).
		RETURNING(Link.AllColumns)

	assertStatementSql(t, stmt, expectedSql, "DuckDuckGo", "http://www.duckduckgo.com", "Ask")

	links := []model.Link{}

	err := stmt.Query(db, &links)

	assert.NilError(t, err)
	assert.Equal(t, len(links), 2)
	assert.Equal(t, links[0].Name, "DuckDuckGo")
	assert.Equal(t, links[1].Name, "DuckDuckGo")
}

func TestUpdateWithSelect(t *testing.T) {

	stmt := Link.UPDATE(Link.AllColumns).
		SET(
			Link.
				SELECT(Link.AllColumns).
				WHERE(Link.ID.EQ(Int(0))),
		).
		WHERE(Link.ID.EQ(Int(0)))

	expectedSql := `
UPDATE test_sample.link
SET (id, url, name, description, rel) = (
     SELECT link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description",
          link.rel AS "link.rel"
     FROM test_sample.link
     WHERE link.id = 0
)
WHERE link.id = 0;
`
	assertStatementSql(t, stmt, expectedSql, int64(0), int64(0))

	assertExec(t, stmt, 1)
}

func TestUpdateWithInvalidSelect(t *testing.T) {

	stmt := Link.UPDATE(Link.AllColumns).
		SET(
			Link.
				SELECT(Link.ID, Link.Name).
				WHERE(Link.ID.EQ(Int(0))),
		).
		WHERE(Link.ID.EQ(Int(0)))

	var expectedSql = `
UPDATE test_sample.link
SET (id, url, name, description, rel) = (
     SELECT link.id AS "link.id",
          link.name AS "link.name"
     FROM test_sample.link
     WHERE link.id = 0
)
WHERE link.id = 0;
`
	assertStatementSql(t, stmt, expectedSql, int64(0), int64(0))

	assertExecErr(t, stmt, "pq: number of columns does not match number of values")
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
		USING(link).
		WHERE(Link.ID.EQ(Int(int64(link.ID))))

	expectedSql := `
UPDATE test_sample.link
SET (id, url, name, description, rel) = (201, 'http://www.duckduckgo.com', 'DuckDuckGo', NULL, NULL)
WHERE link.id = 201;
`
	assertStatementSql(t, stmt, expectedSql, int32(201), "http://www.duckduckgo.com", "DuckDuckGo", nil, nil, int64(201))

	assertExec(t, stmt, 1)
}

func TestUpdateWithModelDataAndPredefinedColumnList(t *testing.T) {

	setupLinkTableForUpdateTest(t)

	link := model.Link{
		ID:   201,
		URL:  "http://www.duckduckgo.com",
		Name: "DuckDuckGo",
	}

	updateColumnList := ColumnList{Link.Rel, Link.Name, Link.URL}

	stmt := Link.
		UPDATE(updateColumnList).
		USING(link).
		WHERE(Link.ID.EQ(Int(int64(link.ID))))

	var expectedSql = `
UPDATE test_sample.link
SET (rel, name, url) = (NULL, 'DuckDuckGo', 'http://www.duckduckgo.com')
WHERE link.id = 201;
`
	assertStatementSql(t, stmt, expectedSql, nil, "DuckDuckGo", "http://www.duckduckgo.com", int64(201))

	assertExec(t, stmt, 1)
}

func TestUpdateWithInvalidModelData(t *testing.T) {
	defer func() {
		r := recover()

		assert.Equal(t, r, "missing struct field for column : id")
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
		USING(link).
		WHERE(Link.ID.EQ(Int(int64(link.Ident))))

	var expectedSql = `
UPDATE test_sample.link
SET (id, url, name, description, rel) = ('http://www.duckduckgo.com', 'DuckDuckGo', NULL, NULL)
WHERE link.id = 201;
`
	assertStatementSql(t, stmt, expectedSql, "http://www.duckduckgo.com", "DuckDuckGo", nil, nil, int64(201))

	assertExecErr(t, stmt, "pq: number of columns does not match number of values")
}

func setupLinkTableForUpdateTest(t *testing.T) {

	cleanUpLinkTable(t)

	_, err := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Rel).
		VALUES(200, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		VALUES(201, "http://www.ask.com", "Ask", DEFAULT).
		VALUES(202, "http://www.ask.com", "Ask", DEFAULT).
		VALUES(203, "http://www.yahoo.com", "Yahoo", DEFAULT).
		VALUES(204, "http://www.bing.com", "Bing", DEFAULT).
		Exec(db)

	assert.NilError(t, err)
}

func cleanUpLinkTable(t *testing.T) {
	_, err := Link.DELETE().WHERE(Link.ID.GT(Int(0))).Exec(db)
	assert.NilError(t, err)
}
