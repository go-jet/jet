package tests

import (
	. "github.com/go-jet/jet"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/table"
	"gotest.tools/assert"
	"testing"
)

func TestDeleteWithWhere(t *testing.T) {
	initForDeleteTest(t)

	var expectedSql = `
DELETE FROM test_sample.link
WHERE link.name IN ('Gmail', 'Outlook');
`
	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))

	assertStatementSql(t, deleteStmt, expectedSql, "Gmail", "Outlook")
	assertExec(t, deleteStmt, 2)
}

func TestDeleteWithWhereAndReturning(t *testing.T) {
	initForDeleteTest(t)

	var expectedSql = `
DELETE FROM test_sample.link
WHERE link.name IN ('Gmail', 'Outlook')
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`
	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook"))).
		RETURNING(Link.AllColumns)

	assertStatementSql(t, deleteStmt, expectedSql, "Gmail", "Outlook")

	dest := []model.Link{}

	err := deleteStmt.Query(db, &dest)

	assert.NilError(t, err)

	assert.Equal(t, len(dest), 2)
	assert.DeepEqual(t, dest[0].Name, "Gmail")
	assert.DeepEqual(t, dest[1].Name, "Outlook")
}

func initForDeleteTest(t *testing.T) {
	cleanUpLinkTable(t)
	stmt := Link.INSERT(Link.URL, Link.Name, Link.Description).
		VALUES("www.gmail.com", "Gmail", "Email service developed by Google").
		VALUES("www.outlook.live.com", "Outlook", "Email service developed by Microsoft")

	assertExec(t, stmt, 2)
}
