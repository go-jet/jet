package postgres

import (
	"context"
	"database/sql"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	model2 "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
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
WHERE link.name IN ('Gmail'::text, 'Outlook'::text);
`, "Gmail", "Outlook")

	testutils.AssertExecAndRollback(t, deleteStmt, db, 2)
	requireQueryLogged(t, deleteStmt, int64(2))
}

func TestDeleteWithWhereAndReturning(t *testing.T) {
	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook"))).
		RETURNING(Link.AllColumns)

	testutils.AssertDebugStatementSql(t, deleteStmt, `
DELETE FROM test_sample.link
WHERE link.name IN ('Gmail'::text, 'Outlook'::text)
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`, "Gmail", "Outlook")

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		var dest []model.Link

		err := deleteStmt.Query(tx, &dest)

		require.NoError(t, err)

		require.Equal(t, len(dest), 2)
		testutils.AssertDeepEqual(t, dest[0].Name, "Gmail")
		testutils.AssertDeepEqual(t, dest[1].Name, "Outlook")
		requireLogged(t, deleteStmt)
	})
}

func TestDeleteQueryContext(t *testing.T) {
	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		dest := []model.Link{}
		err := deleteStmt.QueryContext(ctx, tx, &dest)

		require.Error(t, err, "context deadline exceeded")
		requireLogged(t, deleteStmt)
	})
}

func TestDeleteExecContext(t *testing.T) {
	list := []Expression{String("Gmail"), String("Outlook")}

	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(list...))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		_, err := deleteStmt.ExecContext(ctx, tx)

		require.Error(t, err, "context deadline exceeded")
		requireLogged(t, deleteStmt)
	})
}

func TestDeleteFrom(t *testing.T) {
	skipForCockroachDB(t) // USING is not supported

	stmt := table.Rental.DELETE().
		USING(
			table.Staff.
				INNER_JOIN(table.Store, table.Store.StoreID.EQ(table.Staff.StaffID)),
			table.Actor,
		).
		WHERE(
			AND(
				table.Staff.StaffID.EQ(table.Rental.StaffID),
				table.Store.StoreID.EQ(Int(2)),
				table.Rental.RentalID.LT(Int(10)),
			),
		).
		RETURNING(
			table.Rental.AllColumns,
			table.Store.AllColumns,
		)

	testutils.AssertStatementSql(t, stmt, `
DELETE FROM dvds.rental
USING dvds.staff
     INNER JOIN dvds.store ON (store.store_id = staff.staff_id),
     dvds.actor
WHERE (
          (staff.staff_id = rental.staff_id)
              AND (store.store_id = $1)
              AND (rental.rental_id < $2)
      )
RETURNING rental.rental_id AS "rental.rental_id",
          rental.rental_date AS "rental.rental_date",
          rental.inventory_id AS "rental.inventory_id",
          rental.customer_id AS "rental.customer_id",
          rental.return_date AS "rental.return_date",
          rental.staff_id AS "rental.staff_id",
          rental.last_update AS "rental.last_update",
          store.store_id AS "store.store_id",
          store.manager_staff_id AS "store.manager_staff_id",
          store.address_id AS "store.address_id",
          store.last_update AS "store.last_update";
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
		var dest []struct {
			Rental model2.Rental
			Store  model2.Store
		}

		err := stmt.Query(tx, &dest)

		require.NoError(t, err)
		require.Len(t, dest, 3)
		testutils.AssertJSON(t, dest[0], `
{
	"Rental": {
		"RentalID": 4,
		"RentalDate": "2005-05-24T23:04:41Z",
		"InventoryID": 2452,
		"CustomerID": 333,
		"ReturnDate": "2005-06-03T01:43:41Z",
		"StaffID": 2,
		"LastUpdate": "2006-02-16T02:30:53Z"
	},
	"Store": {
		"StoreID": 2,
		"ManagerStaffID": 2,
		"AddressID": 2,
		"LastUpdate": "2006-02-15T09:57:12Z"
	}
}
`)
	})
}

func TestDeletePreparedStatement(t *testing.T) {
	ctx := context.Background()

	t.Run("tx prep stmt", func(t *testing.T) {
		var txPrepStmt PreparedStatement
		defer txPrepStmt.Close()

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		for i := 200; i < 204; i++ {
			stmt := Link.DELETE().
				WHERE(Link.ID.EQ(Int64(int64(i)))).
				RETURNING(Link.AllColumns)

			err = txPrepStmt.Prepare(ctx, tx, stmt)
			require.NoError(t, err)
			res, err := txPrepStmt.Exec(ctx)
			require.NoError(t, err)
			rowsAffected, err := res.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, rowsAffected, int64(1))
		}
	})

	t.Run("db tx prep stmt", func(t *testing.T) {
		var dbTxPrepStmt PreparedStatement
		defer dbTxPrepStmt.Close()

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		var dest model.Link

		for i := 200; i < 204; i++ {
			stmt := Link.DELETE().
				WHERE(Link.ID.EQ(Int64(int64(i)))).
				RETURNING(Link.AllColumns)

			err = dbTxPrepStmt.Prepare(ctx, db, stmt)
			require.NoError(t, err)
			err = dbTxPrepStmt.Stmt(tx).Query(ctx, &dest)
			require.NoError(t, err)
			require.NotEmpty(t, dest)
		}
	})

	t.Run("rows prep stmt", func(t *testing.T) {
		var prepStmt PreparedStatement
		prepStmt.Close()

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		var dest model.Link

		for i := 200; i < 204; i++ {
			stmt := Link.DELETE().
				WHERE(Link.ID.EQ(Int64(int64(i)))).
				RETURNING(Link.AllColumns)

			err = prepStmt.Prepare(ctx, tx, stmt)
			require.NoError(t, err)
			rows, err := prepStmt.Rows(ctx)
			require.NoError(t, err)

			require.True(t, rows.Next())
			err = rows.Scan(&dest)
			require.NoError(t, err)
			require.NotEmpty(t, dest)
			require.NoError(t, rows.Close())
		}
	})
}
