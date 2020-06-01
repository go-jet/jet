package mysql

import (
	"github.com/go-jet/jet/internal/testutils"
	. "github.com/go-jet/jet/mysql"
	. "github.com/go-jet/jet/tests/.gentestdata/mysql/dvds/table"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestWITH_And_SELECT(t *testing.T) {
	salesRep := CTE("sales_rep")
	salesRepStaffID := Staff.StaffID.From(salesRep)
	salesRepFullName := StringColumn("sales_rep_full_name").From(salesRep)
	customerSalesRep := CTE("customer_sales_rep")

	stmt := WITH(
		salesRep.AS(
			SELECT(
				Staff.StaffID,
				Staff.FirstName.CONCAT(Staff.LastName).AS(salesRepFullName.Name()),
			).FROM(Staff),
		),
		customerSalesRep.AS(
			SELECT(
				Customer.FirstName.CONCAT(Customer.LastName).AS("customer_name"),
				salesRepFullName,
			).FROM(
				salesRep.
					INNER_JOIN(Store, Store.ManagerStaffID.EQ(salesRepStaffID)).
					INNER_JOIN(Customer, Customer.StoreID.EQ(Store.StoreID)),
			),
		),
	)(
		SELECT(customerSalesRep.AllColumns()).
			FROM(customerSalesRep),
	)

	//fmt.Println(stmt.DebugSql())

	testutils.AssertStatementSql(t, stmt, strings.Replace(`
WITH sales_rep AS (
     SELECT staff.staff_id AS "staff.staff_id",
          (CONCAT(staff.first_name, staff.last_name)) AS "sales_rep_full_name"
     FROM dvds.staff
),customer_sales_rep AS (
     SELECT (CONCAT(customer.first_name, customer.last_name)) AS "customer_name",
          sales_rep.sales_rep_full_name AS "sales_rep_full_name"
     FROM sales_rep
          INNER JOIN dvds.store ON (store.manager_staff_id = sales_rep.''staff.staff_id'')
          INNER JOIN dvds.customer ON (customer.store_id = store.store_id)
)
SELECT customer_sales_rep.customer_name AS "customer_name",
     customer_sales_rep.sales_rep_full_name AS "sales_rep_full_name"
FROM customer_sales_rep;
`, "''", "`", -1))

	var dest []struct {
		CustomerName     string
		SalesRepFullName string
	}
	err := stmt.Query(db, &dest)

	require.Equal(t, len(dest), 599)
	require.NoError(t, err)
}

//func TestWITH_And_INSERT(t *testing.T) {
//	paymentsToInsert := CTE("payments_to_insert")
//
//	stmt := WITH(
//		paymentsToInsert.AS(
//			SELECT(Payment.AllColumns).
//				FROM(Payment).
//				WHERE(Payment.Amount.LT(Float(0.5))),
//		),
//	)(
//		Payment.INSERT(Payment.AllColumns).
//			QUERY(
//				SELECT(paymentsToInsert.AllColumns()).
//					FROM(paymentsToInsert),
//			).ON_DUPLICATE_KEY_UPDATE(
//			Payment.PaymentID.SET(Payment.PaymentID.ADD(Int(100000))),
//		),
//	)
//
//	//fmt.Println(stmt.DebugSql())
//
//	tx, err := db.Begin()
//	require.NoError(t, err)
//	defer tx.Rollback()
//
//	testutils.AssertExec(t, stmt, tx, 24)
//}

func TestWITH_And_UPDATE(t *testing.T) {
	paymentsToUpdate := CTE("payments_to_update")
	paymentsToDeleteID := Payment.PaymentID.From(paymentsToUpdate)

	stmt := WITH(
		paymentsToUpdate.AS(
			SELECT(Payment.AllColumns).
				FROM(Payment).
				WHERE(Payment.Amount.LT(Float(0.5))),
		),
	)(
		Payment.UPDATE().
			SET(Payment.Amount.SET(Float(0.0))).
			WHERE(Payment.PaymentID.IN(
				SELECT(paymentsToDeleteID).
					FROM(paymentsToUpdate),
			),
			),
	)

	//fmt.Println(stmt.DebugSql())

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	testutils.AssertExec(t, stmt, tx)
}

func TestWITH_And_DELETE(t *testing.T) {
	paymentsToDelete := CTE("payments_to_delete")
	paymentsToDeleteID := Payment.PaymentID.From(paymentsToDelete)

	stmt := WITH(
		paymentsToDelete.AS(
			SELECT(Payment.AllColumns).
				FROM(Payment).
				WHERE(Payment.Amount.LT(Float(0.5))),
		),
	)(
		Payment.DELETE().
			WHERE(Payment.PaymentID.IN(
				SELECT(paymentsToDeleteID).
					FROM(paymentsToDelete),
			),
			),
	)

	//fmt.Println(stmt.DebugSql())

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	testutils.AssertExec(t, stmt, tx, 24)
}
