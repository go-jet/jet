package mysql

import (
	"github.com/go-jet/jet/internal/testutils"
	. "github.com/go-jet/jet/mysql"
	. "github.com/go-jet/jet/tests/.gentestdata/mysql/dvds/table"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestWITH_SELECT(t *testing.T) {
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

	_, err := stmt.Exec(db)
	require.NoError(t, err)
}
