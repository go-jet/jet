package postgres

import (
	"context"
	. "github.com/go-jet/jet"
	"github.com/go-jet/jet/internal/testutils"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/table"
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestDeleteWithWhere(t *testing.T) {
	initForDeleteTest(t)

	var expectedSQL = `
DELETE FROM test_sample.link
WHERE link.name IN ('Gmail', 'Outlook');
`
	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))

	testutils.AssertDebugStatementSql(t, deleteStmt, expectedSQL, "Gmail", "Outlook")
	assertExec(t, deleteStmt, 2)
}

func TestDeleteWithWhereAndReturning(t *testing.T) {
	initForDeleteTest(t)

	var expectedSQL = `
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

	testutils.AssertDebugStatementSql(t, deleteStmt, expectedSQL, "Gmail", "Outlook")

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

func TestDeleteQueryContext(t *testing.T) {
	initForDeleteTest(t)

	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	dest := []model.Link{}
	err := deleteStmt.QueryContext(ctx, db, &dest)

	assert.Error(t, err, "context deadline exceeded")
}

func TestDeleteExecContext(t *testing.T) {
	initForDeleteTest(t)

	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	_, err := deleteStmt.ExecContext(ctx, db)

	assert.Error(t, err, "context deadline exceeded")
}
