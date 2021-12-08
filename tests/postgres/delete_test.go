package postgres

import (
	"context"
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

func TestDeleteFrom(t *testing.T) {
	tx := beginTx(t)
	defer tx.Rollback()

	stmt := table.Rental.DELETE().
		USING(
			table.Staff.
				INNER_JOIN(table.Store, table.Store.StoreID.EQ(table.Staff.StaffID)),
			table.Actor,
		).WHERE(
		table.Staff.StaffID.EQ(table.Rental.StaffID).
			AND(table.Staff.StaffID.EQ(Int(2))).
			AND(table.Rental.RentalID.LT(Int(10))),
	).RETURNING(
		table.Rental.AllColumns,
		table.Store.AllColumns,
	)

	testutils.AssertStatementSql(t, stmt, `
DELETE FROM dvds.rental
USING dvds.staff
     INNER JOIN dvds.store ON (store.store_id = staff.staff_id),
     dvds.actor
WHERE ((staff.staff_id = rental.staff_id) AND (staff.staff_id = $1)) AND (rental.rental_id < $2)
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
}
