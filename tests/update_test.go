package tests

import (
	"context"
	. "github.com/go-jet/jet"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/table"
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestUpdateValues(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	query := Link.
		UPDATE(Link.Name, Link.URL).
		SET("Bong", "http://bong.com").
		WHERE(Link.Name.EQ(String("Bing")))

	var expectedSQL = `
UPDATE test_sample.link
SET (name, url) = ('Bong', 'http://bong.com')
WHERE link.name = 'Bing';
`
	assertStatementSql(t, query, expectedSQL, "Bong", "http://bong.com", "Bing")

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

	assertStatementSql(t, query, expectedSQL, "Bong", "Bing", "Bing")

	assertExec(t, query, 1)
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

	assertStatementSql(t, stmt, expectedSQL, "DuckDuckGo", "http://www.duckduckgo.com", "Ask")

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
	assertStatementSql(t, stmt, expectedSQL, int64(0), int64(0))

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
	assertStatementSql(t, stmt, expectedSQL, int64(0), int64(0))

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
		MODEL(link).
		WHERE(Link.ID.EQ(Int(int64(link.ID))))

	expectedSQL := `
UPDATE test_sample.link
SET (id, url, name, description) = (201, 'http://www.duckduckgo.com', 'DuckDuckGo', NULL)
WHERE link.id = 201;
`
	assertStatementSql(t, stmt, expectedSQL, int32(201), "http://www.duckduckgo.com", "DuckDuckGo", nil, int64(201))

	assertExec(t, stmt, 1)
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
	assertStatementSql(t, stmt, expectedSQL, nil, "DuckDuckGo", "http://www.duckduckgo.com", int64(201))

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
		MODEL(link).
		WHERE(Link.ID.EQ(Int(int64(link.Ident))))

	var expectedSQL = `
UPDATE test_sample.link
SET (id, url, name, description, rel) = ('http://www.duckduckgo.com', 'DuckDuckGo', NULL, NULL)
WHERE link.id = 201;
`
	assertStatementSql(t, stmt, expectedSQL, "http://www.duckduckgo.com", "DuckDuckGo", nil, nil, int64(201))

	assertExecErr(t, stmt, "pq: number of columns does not match number of values")
}

func TestUpdateQueryContext(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	updateStmt := Link.
		UPDATE(Link.Name, Link.URL).
		SET("Bong", "http://bong.com").
		WHERE(Link.Name.EQ(String("Bing")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Microsecond)

	dest := []model.Link{}
	err := updateStmt.QueryContext(ctx, db, &dest)

	assert.Error(t, err, "context deadline exceeded")
}

func TestUpdateExecContext(t *testing.T) {
	setupLinkTableForUpdateTest(t)

	updateStmt := Link.
		UPDATE(Link.Name, Link.URL).
		SET("Bong", "http://bong.com").
		WHERE(Link.Name.EQ(String("Bing")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Microsecond)

	_, err := updateStmt.ExecContext(ctx, db)

	assert.Error(t, err, "context deadline exceeded")
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

	assert.NilError(t, err)
}

func cleanUpLinkTable(t *testing.T) {
	_, err := Link.DELETE().WHERE(Link.ID.GT(Int(0))).Exec(db)
	assert.NilError(t, err)
}
