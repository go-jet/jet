package postgres

import (
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
