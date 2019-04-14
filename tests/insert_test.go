package tests

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/sub0zero/go-sqlbuilder/sqlbuilder"
	"github.com/sub0zero/go-sqlbuilder/tests/.test_files/dvd_rental/test_sample/model"
	"github.com/sub0zero/go-sqlbuilder/tests/.test_files/dvd_rental/test_sample/table"
	"gotest.tools/assert"
	"testing"
)

func TestInsertValues(t *testing.T) {
	insertQuery := table.Link.INSERT(table.Link.URL, table.Link.Name, table.Link.Rel).
		VALUES("http://www.postgresqltutorial.com", "PostgreSQL Tutorial", sqlbuilder.DEFAULT).
		VALUES("http://www.google.com", "Google", sqlbuilder.DEFAULT).
		VALUES("http://www.yahoo.com", "Yahoo", sqlbuilder.DEFAULT).
		VALUES("http://www.bing.com", "Bing", sqlbuilder.DEFAULT).
		RETURNING(table.Link.ID)

	insertQueryStr, err := insertQuery.String()

	assert.NilError(t, err)

	fmt.Println(insertQueryStr)

	assert.Equal(t, insertQueryStr, `INSERT INTO test_sample.link (url,name,rel) VALUES ('http://www.postgresqltutorial.com','PostgreSQL Tutorial',DEFAULT), ('http://www.google.com','Google',DEFAULT), ('http://www.yahoo.com','Yahoo',DEFAULT), ('http://www.bing.com','Bing',DEFAULT) RETURNING link.id AS "link.id";`)
	res, err := insertQuery.Execute(db)

	assert.NilError(t, err)

	rowsAffected, err := res.RowsAffected()
	assert.NilError(t, err)

	assert.Equal(t, rowsAffected, int64(4))

	link := []model.Link{}

	err = table.Link.SELECT(table.Link.AllColumns).Execute(db, &link)

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

	query := table.Link.
		INSERT(table.Link.URL, table.Link.Name).
		VALUES_MAPPING(linkData)

	queryStr, err := query.String()

	assert.NilError(t, err)

	fmt.Println(queryStr)

	result, err := query.Execute(db)

	assert.NilError(t, err)

	fmt.Println(result)
}

func TestInsertQuery(t *testing.T) {

	_, err := table.Link.INSERT(table.Link.URL, table.Link.Name).
		VALUES("http://www.postgresqltutorial.com", "PostgreSQL Tutorial").Execute(db)

	assert.NilError(t, err)

	query := table.Link.
		INSERT(table.Link.URL, table.Link.Name).
		QUERY(table.Link.SELECT(table.Link.URL, table.Link.Name))

	queryStr, err := query.String()

	assert.NilError(t, err)

	fmt.Println(queryStr)

	_, err = query.Execute(db)

	assert.NilError(t, err)

	allLinks := []model.Link{}
	err = table.Link.SELECT(table.Link.AllColumns).Execute(db, &allLinks)
	assert.NilError(t, err)

	spew.Dump(allLinks)
}
