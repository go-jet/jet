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
WHERE payment.payment_id IN ((
           SELECT payments_to_update.''payment.payment_id'' AS "payment.payment_id"
           FROM payments_to_update
      ));
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
WHERE payment.payment_id IN ((
           SELECT payments_to_delete.''payment.payment_id'' AS "payment.payment_id"
           FROM payments_to_delete
      ));
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

func TestRecursiveWithStatement_Fibonacci(t *testing.T) {
	// CTE columns are listed as part of CTE definition
	n1 := IntegerColumn("n1")
	fibN1 := IntegerColumn("fibN1")
	nextFibN1 := IntegerColumn("nextFibN1")
	fibonacci1 := CTE("fibonacci1", n1, fibN1, nextFibN1)

	// CTE columns are columns from non-recursive select
	fibonacci2 := CTE("fibonacci2")
	n2 := IntegerColumn("n2").From(fibonacci2)
	fibN2 := IntegerColumn("fibN2").From(fibonacci2)
	nextFibN2 := IntegerColumn("nextFibN2").From(fibonacci2)

	stmt := WITH_RECURSIVE(
		fibonacci1.AS(
			SELECT(
				Int32(1), Int32(0), Int32(1),
			).UNION_ALL(
				SELECT(
					n1.ADD(Int(1)), nextFibN1, fibN1.ADD(nextFibN1),
				).FROM(
					fibonacci1,
				).WHERE(
					n1.LT(Int(20)),
				),
			),
		),
		fibonacci2.AS(
			SELECT(
				Int32(1).AS(n2.Name()),
				Int32(0).AS(fibN2.Name()),
				Int32(1).AS(nextFibN2.Name()),
			).UNION_ALL(
				SELECT(
					n2.ADD(Int(1)), nextFibN2, fibN2.ADD(nextFibN2),
				).FROM(
					fibonacci2,
				).WHERE(
					n2.LT(Int(20)),
				),
			),
		),
	)(
		SELECT(
			fibonacci1.AllColumns(),
			fibonacci2.AllColumns(),
		).FROM(
			fibonacci1.INNER_JOIN(fibonacci2, n1.EQ(n2)),
		).WHERE(
			n1.EQ(Int(20)),
		),
	)

	//fmt.Println(stmt.Sql())

	testutils.AssertStatementSql(t, stmt, strings.ReplaceAll(`
WITH RECURSIVE fibonacci1 (n1, ''fibN1'', ''nextFibN1'') AS (
     
     SELECT ?,
          ?,
          ?
     
     UNION ALL
     
     SELECT fibonacci1.n1 + ?,
          fibonacci1.''nextFibN1'' AS "nextFibN1",
          fibonacci1.''fibN1'' + fibonacci1.''nextFibN1''
     FROM fibonacci1
     WHERE fibonacci1.n1 < ?
),fibonacci2 AS (
     
     SELECT ? AS "n2",
          ? AS "fibN2",
          ? AS "nextFibN2"
     
     UNION ALL
     
     SELECT fibonacci2.n2 + ?,
          fibonacci2.''nextFibN2'' AS "nextFibN2",
          fibonacci2.''fibN2'' + fibonacci2.''nextFibN2''
     FROM fibonacci2
     WHERE fibonacci2.n2 < ?
)
SELECT fibonacci1.n1 AS "n1",
     fibonacci1.''fibN1'' AS "fibN1",
     fibonacci1.''nextFibN1'' AS "nextFibN1",
     fibonacci2.n2 AS "n2",
     fibonacci2.''fibN2'' AS "fibN2",
     fibonacci2.''nextFibN2'' AS "nextFibN2"
FROM fibonacci1
     INNER JOIN fibonacci2 ON (fibonacci1.n1 = fibonacci2.n2)
WHERE fibonacci1.n1 = ?;
`, "''", "`"))

	var dest struct {
		N1        int
		FibN1     int
		NextFibN1 int

		N2        int
		FibN2     int
		NextFibN2 int
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	require.Equal(t, dest.N1, 20)
	require.Equal(t, dest.FibN1, 4181)
	require.Equal(t, dest.NextFibN1, 6765)
	require.Equal(t, dest.N2, 20)
	require.Equal(t, dest.FibN2, 4181)
	require.Equal(t, dest.NextFibN2, 6765)
}
