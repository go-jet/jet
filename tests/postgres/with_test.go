package postgres

import (
	"context"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/northwind/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/northwind/table"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWithRegionalSales(t *testing.T) {
	regionalSales := CTE("regional_sales")
	topRegion := CTE("top_region")

	regionalSalesTotalSales := IntegerColumn("total_sales").From(regionalSales)
	regionalSalesShipRegion := Orders.ShipRegion.From(regionalSales)
	topRegionShipRegion := regionalSalesShipRegion.From(topRegion)

	stmt := WITH(
		regionalSales.AS(
			SELECT(
				Orders.ShipRegion,
				SUM(OrderDetails.Quantity).AS(regionalSalesTotalSales.Name()),
			).FROM(
				Orders.INNER_JOIN(OrderDetails, OrderDetails.OrderID.EQ(Orders.OrderID)),
			).GROUP_BY(Orders.ShipRegion),
		),
		topRegion.AS(
			SELECT(
				regionalSalesShipRegion,
			).FROM(
				regionalSales,
			).WHERE(
				regionalSalesTotalSales.GT(
					IntExp(
						SELECT(SUM(regionalSalesTotalSales)).
							FROM(regionalSales),
					).DIV(Int(50)),
				),
			),
		),
	)(
		SELECT(
			Orders.ShipRegion,
			OrderDetails.ProductID,
			COUNT(STAR).AS("product_units"),
			SUM(OrderDetails.Quantity).AS("product_sales"),
		).FROM(
			Orders.
				INNER_JOIN(OrderDetails, Orders.OrderID.EQ(OrderDetails.OrderID)),
		).WHERE(
			Orders.ShipRegion.IN(topRegion.SELECT(topRegionShipRegion)),
		).GROUP_BY(
			Orders.ShipRegion,
			OrderDetails.ProductID,
		).ORDER_BY(
			SUM(OrderDetails.Quantity).DESC(),
		),
	)

	//fmt.Println(stmt.DebugSql())

	testutils.AssertDebugStatementSql(t, stmt, `
WITH regional_sales AS (
     SELECT orders.ship_region AS "orders.ship_region",
          SUM(order_details.quantity) AS "total_sales"
     FROM northwind.orders
          INNER JOIN northwind.order_details ON (order_details.order_id = orders.order_id)
     GROUP BY orders.ship_region
),top_region AS (
     SELECT regional_sales."orders.ship_region" AS "orders.ship_region"
     FROM regional_sales
     WHERE regional_sales.total_sales > ((
               SELECT SUM(regional_sales.total_sales)
               FROM regional_sales
          ) / 50)
)
SELECT orders.ship_region AS "orders.ship_region",
     order_details.product_id AS "order_details.product_id",
     COUNT(*) AS "product_units",
     SUM(order_details.quantity) AS "product_sales"
FROM northwind.orders
     INNER JOIN northwind.order_details ON (orders.order_id = order_details.order_id)
WHERE orders.ship_region IN (
          SELECT top_region."orders.ship_region" AS "orders.ship_region"
          FROM top_region
     )
GROUP BY orders.ship_region, order_details.product_id
ORDER BY SUM(order_details.quantity) DESC;
`)

	_, err := stmt.Exec(db)
	require.NoError(t, err)
}

func TestWithStatementDeleteAndInsert(t *testing.T) {
	removeDiscontinuedOrders := CTE("remove_discontinued_orders")
	updateDiscontinuedPrice := CTE("update_discontinued_price")
	logDiscontinuedProducts := CTE("log_discontinued")

	discontinuedProductID := OrderDetails.ProductID.From(removeDiscontinuedOrders)

	stmt := WITH(
		removeDiscontinuedOrders.AS(
			OrderDetails.DELETE().
				WHERE(OrderDetails.ProductID.IN(
					SELECT(Products.ProductID).
						FROM(Products).
						WHERE(Products.Discontinued.EQ(Int(1)))),
				).RETURNING(OrderDetails.ProductID),
		),
		updateDiscontinuedPrice.AS(
			Products.UPDATE().
				SET(
					Products.UnitPrice.SET(Float(0.0)),
				).
				WHERE(Products.ProductID.IN(removeDiscontinuedOrders.SELECT(discontinuedProductID))).
				RETURNING(Products.AllColumns),
		),
		logDiscontinuedProducts.AS(
			ProductLogs.INSERT(ProductLogs.AllColumns).
				QUERY(SELECT(updateDiscontinuedPrice.AllColumns()).FROM(updateDiscontinuedPrice)).
				RETURNING(
					ProductLogs.ProductID,
					ProductLogs.ProductName,
					ProductLogs.SupplierID,
					ProductLogs.CategoryID,
					ProductLogs.QuantityPerUnit,
					ProductLogs.UnitPrice,
					ProductLogs.UnitsInStock,
					ProductLogs.UnitsOnOrder,
					ProductLogs.ReorderLevel,
					ProductLogs.Discontinued,
				),
		),
	)(
		SELECT(logDiscontinuedProducts.AllColumns()).
			FROM(logDiscontinuedProducts),
	)

	require.Equal(t, len(removeDiscontinuedOrders.AllColumns()), 1)
	require.Equal(t, len(updateDiscontinuedPrice.AllColumns()[0].(ProjectionList)), 10)
	require.Equal(t, len(logDiscontinuedProducts.AllColumns()), 10)

	//fmt.Println(stmt.Sql())

	testutils.AssertStatementSql(t, stmt, `
WITH remove_discontinued_orders AS (
     DELETE FROM northwind.order_details
     WHERE order_details.product_id IN (
               SELECT products.product_id AS "products.product_id"
               FROM northwind.products
               WHERE products.discontinued = $1
          )
     RETURNING order_details.product_id AS "order_details.product_id"
),update_discontinued_price AS (
     UPDATE northwind.products
     SET unit_price = $2
     WHERE products.product_id IN (
               SELECT remove_discontinued_orders."order_details.product_id" AS "order_details.product_id"
               FROM remove_discontinued_orders
          )
     RETURNING products.product_id AS "products.product_id",
               products.product_name AS "products.product_name",
               products.supplier_id AS "products.supplier_id",
               products.category_id AS "products.category_id",
               products.quantity_per_unit AS "products.quantity_per_unit",
               products.unit_price AS "products.unit_price",
               products.units_in_stock AS "products.units_in_stock",
               products.units_on_order AS "products.units_on_order",
               products.reorder_level AS "products.reorder_level",
               products.discontinued AS "products.discontinued"
),log_discontinued AS (
     INSERT INTO northwind.product_logs (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued) (
          SELECT update_discontinued_price."products.product_id" AS "products.product_id",
               update_discontinued_price."products.product_name" AS "products.product_name",
               update_discontinued_price."products.supplier_id" AS "products.supplier_id",
               update_discontinued_price."products.category_id" AS "products.category_id",
               update_discontinued_price."products.quantity_per_unit" AS "products.quantity_per_unit",
               update_discontinued_price."products.unit_price" AS "products.unit_price",
               update_discontinued_price."products.units_in_stock" AS "products.units_in_stock",
               update_discontinued_price."products.units_on_order" AS "products.units_on_order",
               update_discontinued_price."products.reorder_level" AS "products.reorder_level",
               update_discontinued_price."products.discontinued" AS "products.discontinued"
          FROM update_discontinued_price
     )
     RETURNING product_logs.product_id AS "product_logs.product_id",
               product_logs.product_name AS "product_logs.product_name",
               product_logs.supplier_id AS "product_logs.supplier_id",
               product_logs.category_id AS "product_logs.category_id",
               product_logs.quantity_per_unit AS "product_logs.quantity_per_unit",
               product_logs.unit_price AS "product_logs.unit_price",
               product_logs.units_in_stock AS "product_logs.units_in_stock",
               product_logs.units_on_order AS "product_logs.units_on_order",
               product_logs.reorder_level AS "product_logs.reorder_level",
               product_logs.discontinued AS "product_logs.discontinued"
)
SELECT log_discontinued."product_logs.product_id" AS "product_logs.product_id",
     log_discontinued."product_logs.product_name" AS "product_logs.product_name",
     log_discontinued."product_logs.supplier_id" AS "product_logs.supplier_id",
     log_discontinued."product_logs.category_id" AS "product_logs.category_id",
     log_discontinued."product_logs.quantity_per_unit" AS "product_logs.quantity_per_unit",
     log_discontinued."product_logs.unit_price" AS "product_logs.unit_price",
     log_discontinued."product_logs.units_in_stock" AS "product_logs.units_in_stock",
     log_discontinued."product_logs.units_on_order" AS "product_logs.units_on_order",
     log_discontinued."product_logs.reorder_level" AS "product_logs.reorder_level",
     log_discontinued."product_logs.discontinued" AS "product_logs.discontinued"
FROM log_discontinued;
`, int64(1), 0.0)

	var resp []model.ProductLogs

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	err = stmt.Query(tx, &resp)
	require.NoError(t, err)

}

// default column aliases from sub-queries are bubbled up to the main query,
// cte name does not affect default column alias in main query
func TestCTEColumnAliasBubbling(t *testing.T) {
	cte1 := CTE("cte1")
	cte2 := CTE("cte2")

	stmt := WITH(
		cte1.AS(
			SELECT(
				Territories.AllColumns,
				String("custom_column_1").AS("custom_column_1"),
			).FROM(
				Territories,
			).ORDER_BY(
				Territories.TerritoryID.ASC(),
			),
		),
		cte2.AS(
			SELECT(
				cte1.AllColumns(),
				String("custom_column_2").AS("custom_column_2"),
			).FROM(
				cte1,
			),
		),
	)(
		SELECT(
			cte2.AllColumns(),
		).FROM(
			cte2,
		),
	)

	// fmt.Println(stmt.Sql())

	testutils.AssertStatementSql(t, stmt, `
WITH cte1 AS (
     SELECT territories.territory_id AS "territories.territory_id",
          territories.territory_description AS "territories.territory_description",
          territories.region_id AS "territories.region_id",
          $1 AS "custom_column_1"
     FROM northwind.territories
     ORDER BY territories.territory_id ASC
),cte2 AS (
     SELECT cte1."territories.territory_id" AS "territories.territory_id",
          cte1."territories.territory_description" AS "territories.territory_description",
          cte1."territories.region_id" AS "territories.region_id",
          cte1.custom_column_1 AS "custom_column_1",
          $2 AS "custom_column_2"
     FROM cte1
)
SELECT cte2."territories.territory_id" AS "territories.territory_id",
     cte2."territories.territory_description" AS "territories.territory_description",
     cte2."territories.region_id" AS "territories.region_id",
     cte2.custom_column_1 AS "custom_column_1",
     cte2.custom_column_2 AS "custom_column_2"
FROM cte2;
`)

	var dest []struct {
		model.Territories
		CustomColumn1 string
		CustomColumn2 string
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	require.Len(t, dest, 53)
	require.Equal(t, dest[0].Territories, model.Territories{
		TerritoryID:          "01581",
		TerritoryDescription: "Westboro",
		RegionID:             1,
	})
	require.Equal(t, dest[0].CustomColumn1, "custom_column_1")
	require.Equal(t, dest[0].CustomColumn2, "custom_column_2")
}

type EmployeeWrap struct {
	model.Employees

	Subordinates []*EmployeeWrap
}

func TestWithRecursive(t *testing.T) {

	subordinates := CTE("subordinates")

	stmt := WITH_RECURSIVE(
		subordinates.AS(
			SELECT(
				Employees.AllColumns,
			).FROM(
				Employees,
			).WHERE(
				Employees.EmployeeID.EQ(Int(2)),
			).UNION(
				SELECT(
					Employees.AllColumns,
				).FROM(
					Employees.
						INNER_JOIN(subordinates, Employees.EmployeeID.From(subordinates).EQ(Employees.ReportsTo)),
				),
			),
		),
	)(
		SELECT(
			subordinates.AllColumns(),
		).FROM(
			subordinates,
		),
	)

	type employeeID = int16
	employeeMap := make(map[employeeID]*EmployeeWrap)

	rows, err := stmt.Rows(context.Background(), db)
	require.NoError(t, err)

	var result *EmployeeWrap

	for rows.Next() {
		var employeeModel model.Employees
		err := rows.Scan(&employeeModel)
		require.NoError(t, err)

		newEmployeeWrap := &EmployeeWrap{
			Employees: employeeModel,
		}

		employeeMap[employeeModel.EmployeeID] = newEmployeeWrap

		if employeeModel.ReportsTo == nil { // top manager(always first row in the result)
			result = newEmployeeWrap
			continue
		}

		if employee, ok := employeeMap[*employeeModel.ReportsTo]; ok {
			employee.Subordinates = append(employee.Subordinates, newEmployeeWrap)
		}
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	testutils.AssertJSON(t, *result, `
{
	"EmployeeID": 2,
	"LastName": "Fuller",
	"FirstName": "Andrew",
	"Title": "Vice President, Sales",
	"TitleOfCourtesy": "Dr.",
	"BirthDate": "1952-02-19T00:00:00Z",
	"HireDate": "1992-08-14T00:00:00Z",
	"Address": "908 W. Capital Way",
	"City": "Tacoma",
	"Region": "WA",
	"PostalCode": "98401",
	"Country": "USA",
	"HomePhone": "(206) 555-9482",
	"Extension": "3457",
	"Photo": "",
	"Notes": "Andrew received his BTS commercial in 1974 and a Ph.D. in international marketing from the University of Dallas in 1981.  He is fluent in French and Italian and reads German.  He joined the company as a sales representative, was promoted to sales manager in January 1992 and to vice president of sales in March 1993.  Andrew is a member of the Sales Management Roundtable, the Seattle Chamber of Commerce, and the Pacific Rim Importers Association.",
	"ReportsTo": null,
	"PhotoPath": "http://accweb/emmployees/fuller.bmp",
	"Subordinates": [
		{
			"EmployeeID": 1,
			"LastName": "Davolio",
			"FirstName": "Nancy",
			"Title": "Sales Representative",
			"TitleOfCourtesy": "Ms.",
			"BirthDate": "1948-12-08T00:00:00Z",
			"HireDate": "1992-05-01T00:00:00Z",
			"Address": "507 - 20th Ave. E.\\nApt. 2A",
			"City": "Seattle",
			"Region": "WA",
			"PostalCode": "98122",
			"Country": "USA",
			"HomePhone": "(206) 555-9857",
			"Extension": "5467",
			"Photo": "",
			"Notes": "Education includes a BA in psychology from Colorado State University in 1970.  She also completed The Art of the Cold Call.  Nancy is a member of Toastmasters International.",
			"ReportsTo": 2,
			"PhotoPath": "http://accweb/emmployees/davolio.bmp",
			"Subordinates": null
		},
		{
			"EmployeeID": 3,
			"LastName": "Leverling",
			"FirstName": "Janet",
			"Title": "Sales Representative",
			"TitleOfCourtesy": "Ms.",
			"BirthDate": "1963-08-30T00:00:00Z",
			"HireDate": "1992-04-01T00:00:00Z",
			"Address": "722 Moss Bay Blvd.",
			"City": "Kirkland",
			"Region": "WA",
			"PostalCode": "98033",
			"Country": "USA",
			"HomePhone": "(206) 555-3412",
			"Extension": "3355",
			"Photo": "",
			"Notes": "Janet has a BS degree in chemistry from Boston College (1984).  She has also completed a certificate program in food retailing management.  Janet was hired as a sales associate in 1991 and promoted to sales representative in February 1992.",
			"ReportsTo": 2,
			"PhotoPath": "http://accweb/emmployees/leverling.bmp",
			"Subordinates": null
		},
		{
			"EmployeeID": 4,
			"LastName": "Peacock",
			"FirstName": "Margaret",
			"Title": "Sales Representative",
			"TitleOfCourtesy": "Mrs.",
			"BirthDate": "1937-09-19T00:00:00Z",
			"HireDate": "1993-05-03T00:00:00Z",
			"Address": "4110 Old Redmond Rd.",
			"City": "Redmond",
			"Region": "WA",
			"PostalCode": "98052",
			"Country": "USA",
			"HomePhone": "(206) 555-8122",
			"Extension": "5176",
			"Photo": "",
			"Notes": "Margaret holds a BA in English literature from Concordia College (1958) and an MA from the American Institute of Culinary Arts (1966).  She was assigned to the London office temporarily from July through November 1992.",
			"ReportsTo": 2,
			"PhotoPath": "http://accweb/emmployees/peacock.bmp",
			"Subordinates": null
		},
		{
			"EmployeeID": 5,
			"LastName": "Buchanan",
			"FirstName": "Steven",
			"Title": "Sales Manager",
			"TitleOfCourtesy": "Mr.",
			"BirthDate": "1955-03-04T00:00:00Z",
			"HireDate": "1993-10-17T00:00:00Z",
			"Address": "14 Garrett Hill",
			"City": "London",
			"Region": null,
			"PostalCode": "SW1 8JR",
			"Country": "UK",
			"HomePhone": "(71) 555-4848",
			"Extension": "3453",
			"Photo": "",
			"Notes": "Steven Buchanan graduated from St. Andrews University, Scotland, with a BSC degree in 1976.  Upon joining the company as a sales representative in 1992, he spent 6 months in an orientation program at the Seattle office and then returned to his permanent post in London.  He was promoted to sales manager in March 1993.  Mr. Buchanan has completed the courses Successful Telemarketing and International Sales Management.  He is fluent in French.",
			"ReportsTo": 2,
			"PhotoPath": "http://accweb/emmployees/buchanan.bmp",
			"Subordinates": [
				{
					"EmployeeID": 6,
					"LastName": "Suyama",
					"FirstName": "Michael",
					"Title": "Sales Representative",
					"TitleOfCourtesy": "Mr.",
					"BirthDate": "1963-07-02T00:00:00Z",
					"HireDate": "1993-10-17T00:00:00Z",
					"Address": "Coventry House\\nMiner Rd.",
					"City": "London",
					"Region": null,
					"PostalCode": "EC2 7JR",
					"Country": "UK",
					"HomePhone": "(71) 555-7773",
					"Extension": "428",
					"Photo": "",
					"Notes": "Michael is a graduate of Sussex University (MA, economics, 1983) and the University of California at Los Angeles (MBA, marketing, 1986).  He has also taken the courses Multi-Cultural Selling and Time Management for the Sales Professional.  He is fluent in Japanese and can read and write French, Portuguese, and Spanish.",
					"ReportsTo": 5,
					"PhotoPath": "http://accweb/emmployees/davolio.bmp",
					"Subordinates": null
				},
				{
					"EmployeeID": 7,
					"LastName": "King",
					"FirstName": "Robert",
					"Title": "Sales Representative",
					"TitleOfCourtesy": "Mr.",
					"BirthDate": "1960-05-29T00:00:00Z",
					"HireDate": "1994-01-02T00:00:00Z",
					"Address": "Edgeham Hollow\\nWinchester Way",
					"City": "London",
					"Region": null,
					"PostalCode": "RG1 9SP",
					"Country": "UK",
					"HomePhone": "(71) 555-5598",
					"Extension": "465",
					"Photo": "",
					"Notes": "Robert King served in the Peace Corps and traveled extensively before completing his degree in English at the University of Michigan in 1992, the year he joined the company.  After completing a course entitled Selling in Europe, he was transferred to the London office in March 1993.",
					"ReportsTo": 5,
					"PhotoPath": "http://accweb/emmployees/davolio.bmp",
					"Subordinates": null
				},
				{
					"EmployeeID": 9,
					"LastName": "Dodsworth",
					"FirstName": "Anne",
					"Title": "Sales Representative",
					"TitleOfCourtesy": "Ms.",
					"BirthDate": "1966-01-27T00:00:00Z",
					"HireDate": "1994-11-15T00:00:00Z",
					"Address": "7 Houndstooth Rd.",
					"City": "London",
					"Region": null,
					"PostalCode": "WG2 7LT",
					"Country": "UK",
					"HomePhone": "(71) 555-4444",
					"Extension": "452",
					"Photo": "",
					"Notes": "Anne has a BA degree in English from St. Lawrence College.  She is fluent in French and German.",
					"ReportsTo": 5,
					"PhotoPath": "http://accweb/emmployees/davolio.bmp",
					"Subordinates": null
				}
			]
		},
		{
			"EmployeeID": 8,
			"LastName": "Callahan",
			"FirstName": "Laura",
			"Title": "Inside Sales Coordinator",
			"TitleOfCourtesy": "Ms.",
			"BirthDate": "1958-01-09T00:00:00Z",
			"HireDate": "1994-03-05T00:00:00Z",
			"Address": "4726 - 11th Ave. N.E.",
			"City": "Seattle",
			"Region": "WA",
			"PostalCode": "98105",
			"Country": "USA",
			"HomePhone": "(206) 555-1189",
			"Extension": "2344",
			"Photo": "",
			"Notes": "Laura received a BA in psychology from the University of Washington.  She has also completed a course in business French.  She reads and writes French.",
			"ReportsTo": 2,
			"PhotoPath": "http://accweb/emmployees/davolio.bmp",
			"Subordinates": null
		}
	]
}
`)
}
