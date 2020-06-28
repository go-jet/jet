package postgres

import (
	"context"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
	"github.com/stretchr/testify/require"
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
	AssertExec(t, deleteStmt, 2)
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

	require.NoError(t, err)

	require.Equal(t, len(dest), 2)
	testutils.AssertDeepEqual(t, dest[0].Name, "Gmail")
	testutils.AssertDeepEqual(t, dest[1].Name, "Outlook")
	requireLogged(t, deleteStmt)
}

func initForDeleteTest(t *testing.T) {
	cleanUpLinkTable(t)
	stmt := Link.INSERT(Link.URL, Link.Name, Link.Description).
		VALUES("www.gmail.com", "Gmail", "Email service developed by Google").
		VALUES("www.outlook.live.com", "Outlook", "Email service developed by Microsoft")

	AssertExec(t, stmt, 2)
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

	require.Error(t, err, "context deadline exceeded")
	requireLogged(t, deleteStmt)
}

func TestDeleteExecContext(t *testing.T) {
	initForDeleteTest(t)

	list := []Expression{String("Gmail"), String("Outlook")}

	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(list...))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	_, err := deleteStmt.ExecContext(ctx, db)

	require.Error(t, err, "context deadline exceeded")
	requireLogged(t, deleteStmt)
}
