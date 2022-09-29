package mysql

import (
	"context"
	"database/sql"
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
	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))

	testutils.AssertDebugStatementSql(t, deleteStmt, `
DELETE FROM test_sample.link
WHERE link.name IN ('Gmail', 'Outlook');
`, "Gmail", "Outlook")

	testutils.AssertExecAndRollback(t, deleteStmt, db, 2)
	requireLogged(t, deleteStmt)
}

func TestDeleteWithWhereOrderByLimit(t *testing.T) {
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
	testutils.AssertExecAndRollback(t, deleteStmt, db, 1)
	requireLogged(t, deleteStmt)
}

func TestDeleteQueryContext(t *testing.T) {
	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	var dest []model.Link
	err := deleteStmt.QueryContext(ctx, db, &dest)

	require.Error(t, err, "context deadline exceeded")
	requireLogged(t, deleteStmt)
}

func TestDeleteExecContext(t *testing.T) {
	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	_, err := deleteStmt.ExecContext(ctx, db)

	require.Error(t, err, "context deadline exceeded")
}

func TestDeleteWithUsing(t *testing.T) {
	stmt := table.Rental.DELETE().
		USING(
			table.Rental.
				INNER_JOIN(table.Staff, table.Rental.StaffID.EQ(table.Staff.StaffID)),
			table.Actor,
		).
		WHERE(
			table.Staff.StaffID.NOT_EQ(Int(2)).
				AND(table.Rental.RentalID.LT(Int(100))),
		)

	testutils.AssertStatementSql(t, stmt, `
DELETE FROM dvds.rental
USING dvds.rental
     INNER JOIN dvds.staff ON (rental.staff_id = staff.staff_id),
     dvds.actor
WHERE (staff.staff_id != ?) AND (rental.rental_id < ?);
`)

	testutils.AssertExecAndRollback(t, stmt, db)
}

func TestDeleteOptimizerHints(t *testing.T) {

	stmt := Link.DELETE().
		OPTIMIZER_HINTS(QB_NAME("deleteIns"), "MRR(link)").
		WHERE(
			Link.Name.IN(String("Gmail"), String("Outlook")),
		)

	testutils.AssertDebugStatementSql(t, stmt, `
DELETE /*+ QB_NAME(deleteIns) MRR(link) */ FROM test_sample.link
WHERE link.name IN ('Gmail', 'Outlook');
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := stmt.Exec(tx)
		require.NoError(t, err)
	})
}
