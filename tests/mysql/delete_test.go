package mysql

import (
	"context"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/table"
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
	testutils.AssertExec(t, deleteStmt, db, 2)
	requireLogged(t, deleteStmt)
}

func TestDeleteWithWhereOrderByLimit(t *testing.T) {
	initForDeleteTest(t)

	var expectedSQL = `
DELETE FROM test_sample.link
WHERE link.name IN ('Gmail', 'Outlook')
ORDER BY link.name
LIMIT 1;
`
	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook"))).
		ORDER_BY(Link.Name).
		LIMIT(1)

	testutils.AssertDebugStatementSql(t, deleteStmt, expectedSQL, "Gmail", "Outlook", int64(1))
	testutils.AssertExec(t, deleteStmt, db, 1)
	requireLogged(t, deleteStmt)
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

	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	_, err := deleteStmt.ExecContext(ctx, db)

	require.Error(t, err, "context deadline exceeded")
}

func initForDeleteTest(t *testing.T) {
	cleanUpLinkTable(t)
	stmt := Link.INSERT(Link.URL, Link.Name, Link.Description).
		VALUES("www.gmail.com", "Gmail", "Email service developed by Google").
		VALUES("www.outlook.live.com", "Outlook", "Email service developed by Microsoft")

	testutils.AssertExec(t, stmt, db, 2)
}

func TestDeleteWithUsing(t *testing.T) {
	tx := beginTx(t)
	defer tx.Rollback()

	stmt := table.Rental.DELETE().
		USING(
			table.Rental.
				INNER_JOIN(table.Staff, table.Rental.StaffID.EQ(table.Staff.StaffID)),
			table.Actor,
		).WHERE(
		table.Staff.StaffID.EQ(Int(2)).
			AND(table.Rental.RentalID.LT(Int(10))),
	)

	testutils.AssertStatementSql(t, stmt, `
DELETE FROM dvds.rental
USING dvds.rental
     INNER JOIN dvds.staff ON (rental.staff_id = staff.staff_id),
     dvds.actor
WHERE (staff.staff_id = ?) AND (rental.rental_id < ?);
`)

	_, err := stmt.Exec(tx)
	require.NoError(t, err)
}
