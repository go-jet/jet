package postgres

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/northwind/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/northwind/table"
	"github.com/stretchr/testify/require"
	"testing"
)

type Dest []struct {
	model.Customers

	Demographics model.CustomerDemographics

	Orders []struct {
		model.Orders

		Shipper model.Shippers

		Employee struct {
			model.Employees

			Territories []struct {
				model.Territories

				Region model.Region
			}
		}

		Details []struct {
			model.OrderDetails

			Products struct {
				model.Products

				Category model.Categories
				Supplier model.Suppliers
			}
		}
	}
}

func BenchmarkTestNorthwindJoinEverything(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testNorthwindJoinEverything(b)
	}
}

func TestTestNorthwindJoinEverything(t *testing.T) {
	testNorthwindJoinEverything(t)
}

func testNorthwindJoinEverything(t require.TestingT) {

	stmt :=
		SELECT(
			Customers.AllColumns,
			CustomerDemographics.AllColumns,
			Orders.AllColumns,
			Shippers.AllColumns,
			OrderDetails.AllColumns,
			Products.AllColumns,
			Categories.AllColumns,
			Suppliers.AllColumns,
			Employees.AllColumns,
			Territories.AllColumns,
			Region.AllColumns,
		).FROM(
			Customers.
				LEFT_JOIN(CustomerCustomerDemo, Customers.CustomerID.EQ(CustomerCustomerDemo.CustomerID)).
				LEFT_JOIN(CustomerDemographics, CustomerCustomerDemo.CustomerTypeID.EQ(CustomerDemographics.CustomerTypeID)).
				LEFT_JOIN(Orders, Orders.CustomerID.EQ(Customers.CustomerID)).
				LEFT_JOIN(Shippers, Orders.ShipVia.EQ(Shippers.ShipperID)).
				LEFT_JOIN(OrderDetails, Orders.OrderID.EQ(OrderDetails.OrderID)).
				LEFT_JOIN(Products, OrderDetails.ProductID.EQ(Products.ProductID)).
				LEFT_JOIN(Categories, Products.CategoryID.EQ(Categories.CategoryID)).
				LEFT_JOIN(Suppliers, Products.SupplierID.EQ(Suppliers.SupplierID)).
				LEFT_JOIN(Employees, Orders.EmployeeID.EQ(Employees.EmployeeID)).
				LEFT_JOIN(EmployeeTerritories, EmployeeTerritories.EmployeeID.EQ(Employees.EmployeeID)).
				LEFT_JOIN(Territories, EmployeeTerritories.TerritoryID.EQ(Territories.TerritoryID)).
				LEFT_JOIN(Region, Territories.RegionID.EQ(Region.RegionID)),
		).ORDER_BY(
			Customers.CustomerID,
			Orders.OrderID,
			Products.ProductID,
			Territories.TerritoryID,
		)

	//fmt.Println(stmt.DebugSql())

	var dest Dest

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	//testutils.SaveJSONFile(dest, "./testdata/results/postgres/northwind-all.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/northwind-all.json")
	requireLogged(t, stmt)
}

func BenchmarkTestNorthwindJoinEverythingJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testNorthwindJoinEverythingJson(b)
	}
}

func TestNorthwindJoinEverythingJson(t *testing.T) {
	testNorthwindJoinEverythingJson(t)
}

func testNorthwindJoinEverythingJson(t require.TestingT) {

	stmt := SELECT_JSON_ARR(
		Customers.AllColumns,

		SELECT_JSON_OBJ(CustomerDemographics.AllColumns).
			FROM(CustomerDemographics.INNER_JOIN(CustomerCustomerDemo, CustomerCustomerDemo.CustomerTypeID.EQ(CustomerDemographics.CustomerTypeID))).
			WHERE(CustomerCustomerDemo.CustomerID.EQ(Customers.CustomerID)).AS("Demographics"),

		SELECT_JSON_ARR(
			Orders.AllColumns,

			SELECT_JSON_OBJ(Shippers.AllColumns).
				FROM(Shippers).
				WHERE(Shippers.ShipperID.EQ(Orders.ShipVia)).AS("Shipper"),

			SELECT_JSON_OBJ(
				Employees.AllColumns,
				SELECT_JSON_ARR(
					Territories.AllColumns,

					SELECT_JSON_OBJ(Region.AllColumns).
						FROM(Region).
						WHERE(Region.RegionID.EQ(Territories.RegionID)).AS("Region"),
				).FROM(
					EmployeeTerritories.LEFT_JOIN(
						Territories,
						EmployeeTerritories.TerritoryID.EQ(Territories.TerritoryID)),
				).WHERE(
					EmployeeTerritories.EmployeeID.EQ(Employees.EmployeeID), // TODO: move to join
				).AS("Territories"),
			).FROM(Employees).
				WHERE(Orders.EmployeeID.EQ(Employees.EmployeeID)).AS("Employee"),

			SELECT_JSON_ARR(
				OrderDetails.AllColumns,

				SELECT_JSON_OBJ(
					Products.AllColumns,

					SELECT_JSON_OBJ(
						Categories.AllColumns,
					).FROM(Categories).
						WHERE(Categories.CategoryID.EQ(Products.CategoryID)).AS("Category"),

					SELECT_JSON_OBJ(Suppliers.AllColumns).
						FROM(Suppliers).
						WHERE(Suppliers.SupplierID.EQ(Products.SupplierID)).AS("Supplier"),
				).FROM(Products).
					WHERE(Products.ProductID.EQ(OrderDetails.ProductID)).AS("Products"),
			).FROM(
				OrderDetails,
			).WHERE(
				OrderDetails.OrderID.EQ(Orders.OrderID),
			).AS("Details"),
		).FROM(
			Orders,
		).WHERE(
			Orders.CustomerID.EQ(Customers.CustomerID),
		).ORDER_BY(
			Orders.OrderID,
		).AS("Orders"),
	).FROM(
		Customers,
	).ORDER_BY(
		Customers.CustomerID,
	)

	//fmt.Println(stmt.DebugSql())

	var dest Dest

	err := stmt.QueryJSON(ctx, db, &dest)
	require.NoError(t, err)

	//testutils.SaveJSONFile(dest, "./testdata/results/postgres/northwind-all2.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/northwind-all.json")
}
