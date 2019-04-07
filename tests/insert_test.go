package tests

import (
	"fmt"
	"github.com/sub0zero/go-sqlbuilder/tests/.test_files/dvd_rental/test_sample/model"
	"github.com/sub0zero/go-sqlbuilder/tests/.test_files/dvd_rental/test_sample/table"
	"gotest.tools/assert"
	"testing"
)

func TestInsertValues(t *testing.T) {
	insertQuery := table.Link.INSERT(table.Link.URL, table.Link.Name).
		VALUES("http://www.postgresqltutorial.com", "PostgreSQL Tutorial").
		VALUES("http://www.google.com", "Google").
		VALUES("http://www.yahoo.com", "Yahoo").
		VALUES("http://www.bing.com", "Bing").
		RETURNING(table.Link.ID)

	insertQueryStr, err := insertQuery.String()

	assert.NilError(t, err)

	fmt.Println(insertQueryStr)

	assert.Equal(t, insertQueryStr, `INSERT INTO test_sample.link (url,name) VALUES ('http://www.postgresqltutorial.com','PostgreSQL Tutorial'), ('http://www.google.com','Google'), ('http://www.yahoo.com','Yahoo'), ('http://www.bing.com','Bing') RETURNING link.id;`)
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

	query := table.Link.INSERT(table.Link.URL, table.Link.Name).
		VALUES_MAPPING(linkData)

	queryStr, err := query.String()

	assert.NilError(t, err)

	fmt.Println(queryStr)

	result, err := query.Execute(db)

	assert.NilError(t, err)

	fmt.Println(result)
}
