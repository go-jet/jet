package tests

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	. "github.com/go-jet/jet/sqlbuilder"
	"github.com/go-jet/jet/tests/.test_files/dvd_rental/test_sample/model"
	. "github.com/go-jet/jet/tests/.test_files/dvd_rental/test_sample/table"
	"gotest.tools/assert"
	"testing"
)

func TestInsertValues(t *testing.T) {
	insertQuery := Link.INSERT(Link.URL, Link.Name, Link.Rel).
		VALUES("http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		VALUES("http://www.google.com", "Google", DEFAULT).
		VALUES("http://www.yahoo.com", "Yahoo", DEFAULT).
		VALUES("http://www.bing.com", "Bing", DEFAULT).
		RETURNING(Link.ID)

	insertQueryStr, args, err := insertQuery.Sql()

	assert.NilError(t, err)
	assert.Equal(t, len(args), 8)

	fmt.Println(insertQueryStr)

	assert.Equal(t, insertQueryStr, `
INSERT INTO test_sample.link (url,name,rel) VALUES
     ($1, $2, DEFAULT),
     ($3, $4, DEFAULT),
     ($5, $6, DEFAULT),
     ($7, $8, DEFAULT)
RETURNING link.id AS "link.id";
`)
	res, err := insertQuery.Execute(db)

	assert.NilError(t, err)

	rowsAffected, err := res.RowsAffected()
	assert.NilError(t, err)

	assert.Equal(t, rowsAffected, int64(4))

	link := []model.Link{}

	err = Link.SELECT(Link.AllColumns).Query(db, &link)

	assert.NilError(t, err)

	assert.Equal(t, len(link), 4)

	assert.DeepEqual(t, link[0], model.Link{
		ID:   1,
		URL:  "http://www.postgresqltutorial.com",
		Name: "PostgreSQL Tutorial",
		Rel:  nil,
	})

	assert.DeepEqual(t, link[3], model.Link{
		ID:   4,
		URL:  "http://www.bing.com",
		Name: "Bing",
		Rel:  nil,
	})
}

func TestInsertDataObject(t *testing.T) {
	linkData := model.Link{
		URL:  "http://www.duckduckgo.com",
		Name: "Duck Duck go",
		Rel:  nil,
	}

	query := Link.
		INSERT(Link.URL, Link.Name).
		MODEL(linkData)

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	assert.Equal(t, len(args), 2)

	fmt.Println(queryStr)

	result, err := query.Execute(db)

	assert.NilError(t, err)

	fmt.Println(result)
}

func TestInsertQuery(t *testing.T) {

	_, err := Link.INSERT(Link.URL, Link.Name).
		VALUES("http://www.postgresqltutorial.com", "PostgreSQL Tutorial").Execute(db)

	assert.NilError(t, err)

	query := Link.
		INSERT(Link.URL, Link.Name).
		QUERY(Link.SELECT(Link.URL, Link.Name))

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	assert.Equal(t, len(args), 0)

	fmt.Println(queryStr)

	_, err = query.Execute(db)

	assert.NilError(t, err)

	allLinks := []model.Link{}
	err = Link.SELECT(Link.AllColumns).Query(db, &allLinks)
	assert.NilError(t, err)

	spew.Dump(allLinks)
}
