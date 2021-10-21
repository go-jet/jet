package sqlite

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/sqlite"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/table"
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

	testutils.AssertStatementSql(t, stmt, strings.Replace(`
WITH sales_rep AS (
     SELECT staff.staff_id AS "staff.staff_id",
          (staff.first_name || staff.last_name) AS "sales_rep_full_name"
     FROM staff
),customer_sales_rep AS (
     SELECT (customer.first_name || customer.last_name) AS "customer_name",
          sales_rep.sales_rep_full_name AS "sales_rep_full_name"
     FROM sales_rep
          INNER JOIN store ON (store.manager_staff_id = sales_rep.''staff.staff_id'')
          INNER JOIN customer ON (customer.store_id = store.store_id)
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
	require.NoError(t, err)
	require.Equal(t, len(dest), 599)
}

func TestWITH_And_INSERT(t *testing.T) {
	paymentsToInsert := CTE("payments_to_insert")

	stmt := WITH(
		paymentsToInsert.AS(
			SELECT(Payment.AllColumns).
				FROM(Payment).
				WHERE(Payment.Amount.LT(Float(0.5))),
		),
	)(
		Payment.INSERT(Payment.AllColumns).
			QUERY(
				SELECT(
					paymentsToInsert.AllColumns(),
				).FROM(
					paymentsToInsert,
				).WHERE(Bool(true)), //https://stackoverflow.com/questions/66230093/error-while-doing-upsert-in-sqlite-3-34-error-near-do-syntax-error
			).ON_CONFLICT().DO_UPDATE(
			SET(
				Payment.PaymentID.SET(Payment.PaymentID.ADD(Int(100000))),
			),
		),
	)

	testutils.AssertDebugStatementSql(t, stmt, strings.Replace(`
WITH payments_to_insert AS (
     SELECT payment.payment_id AS "payment.payment_id",
          payment.customer_id AS "payment.customer_id",
          payment.staff_id AS "payment.staff_id",
          payment.rental_id AS "payment.rental_id",
          payment.amount AS "payment.amount",
          payment.payment_date AS "payment.payment_date",
          payment.last_update AS "payment.last_update"
     FROM payment
     WHERE payment.amount < 0.5
)
INSERT INTO payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date, last_update)
SELECT payments_to_insert.''payment.payment_id'' AS "payment.payment_id",
     payments_to_insert.''payment.customer_id'' AS "payment.customer_id",
     payments_to_insert.''payment.staff_id'' AS "payment.staff_id",
     payments_to_insert.''payment.rental_id'' AS "payment.rental_id",
     payments_to_insert.''payment.amount'' AS "payment.amount",
     payments_to_insert.''payment.payment_date'' AS "payment.payment_date",
     payments_to_insert.''payment.last_update'' AS "payment.last_update"
FROM payments_to_insert
WHERE TRUE
ON CONFLICT DO UPDATE
       SET payment_id = (payment.payment_id + 100000);
`, "''", "`", -1))

	tx := beginDBTx(t)
	defer tx.Rollback()

	testutils.AssertExec(t, stmt, tx, 24)
}

func TestWITH_SELECT_UPDATE(t *testing.T) {
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

	testutils.AssertDebugStatementSql(t, stmt, strings.Replace(`
WITH payments_to_update AS (
     SELECT payment.payment_id AS "payment.payment_id",
          payment.customer_id AS "payment.customer_id",
          payment.staff_id AS "payment.staff_id",
          payment.rental_id AS "payment.rental_id",
          payment.amount AS "payment.amount",
          payment.payment_date AS "payment.payment_date",
          payment.last_update AS "payment.last_update"
     FROM payment
     WHERE payment.amount < 0.5
)
UPDATE payment
SET amount = 0
WHERE payment.payment_id IN (
          SELECT payments_to_update.''payment.payment_id'' AS "payment.payment_id"
          FROM payments_to_update
     );
`, "''", "`", -1))

	tx := beginDBTx(t)
	defer tx.Rollback()

	testutils.AssertExec(t, stmt, tx)
}

func TestWITH_And_DELETE(t *testing.T) {
	paymentsToDelete := CTE("payments_to_delete")
	paymentsToDeleteID := Payment.PaymentID.From(paymentsToDelete)

	stmt := WITH(
		paymentsToDelete.AS(
			SELECT(
				Payment.AllColumns,
			).FROM(
				Payment,
			).WHERE(
				Payment.Amount.LT(Float(0.5)),
			),
		),
	)(
		Payment.DELETE().
			WHERE(
				Payment.PaymentID.IN(
					SELECT(
						paymentsToDeleteID,
					).FROM(
						paymentsToDelete,
					),
				),
			),
	)

	testutils.AssertDebugStatementSql(t, stmt, strings.Replace(`
WITH payments_to_delete AS (
     SELECT payment.payment_id AS "payment.payment_id",
          payment.customer_id AS "payment.customer_id",
          payment.staff_id AS "payment.staff_id",
          payment.rental_id AS "payment.rental_id",
          payment.amount AS "payment.amount",
          payment.payment_date AS "payment.payment_date",
          payment.last_update AS "payment.last_update"
     FROM payment
     WHERE payment.amount < 0.5
)
DELETE FROM payment
WHERE payment.payment_id IN (
          SELECT payments_to_delete.''payment.payment_id'' AS "payment.payment_id"
          FROM payments_to_delete
     );
`, "''", "`", -1))

	tx := beginDBTx(t)
	defer tx.Rollback()

	testutils.AssertExec(t, stmt, tx, 24)
}

func TestOperatorIN(t *testing.T) {
	stmt := SELECT(Payment.PaymentID.IN(SELECT(Int(11)), Int(22))).
		FROM(Payment)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT payment.payment_id IN ((
          SELECT 11
     ), 22)
FROM payment;
`)

	var dest []struct{}
	err := stmt.Query(db, &dest)
	require.NoError(t, err)
}
