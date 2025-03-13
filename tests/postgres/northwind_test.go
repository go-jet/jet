package postgres

import (
	"github.com/bytedance/sonic"
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

func BenchmarkTestNorthwindJoinEverythingSonicJson(b *testing.B) {
	useJsonUnmarshalFunc(sonic.Unmarshal, func() {
		for i := 0; i < b.N; i++ {
			testNorthwindJoinEverythingJson(b)
		}
	})
}

// uncomment when bug is fixed: https://github.com/bytedance/sonic/issues/774
//func TestNorthwindJoinEverythingJsonSonic(t *testing.T) {
//	useJsonUnmarshalFunc(sonic.Unmarshal, func() {
//		testNorthwindJoinEverythingJson(t)
//	})
//}

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
					EmployeeTerritories.EmployeeID.EQ(Employees.EmployeeID),
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

	err := stmt.QueryContext(ctx, db, &dest)
	require.NoError(t, err)

	//testutils.SaveJSONFile(dest, "./testdata/results/postgres/northwind-all2.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/northwind-all.json")
}

func TestColumnListFrom(t *testing.T) {

	toOrderBy := map[string]bool{
		"city":        true,
		"region":      true,
		"postal_code": true,
	}

	subQuery := SELECT(
		Customers.AllColumns,
	).FROM(
		Customers,
	).AsTable("subQuery")

	var orderBy []OrderByClause

	for _, column := range Customers.AllColumns.From(subQuery) {
		if toOrderBy[column.Name()] {
			orderBy = append(orderBy, column.ASC())
		}
	}

	stmt := SELECT(
		subQuery.AllColumns(),
	).FROM(
		subQuery,
	).ORDER_BY(
		orderBy...,
	).LIMIT(3)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "subQuery"."customers.customer_id" AS "customers.customer_id",
     "subQuery"."customers.company_name" AS "customers.company_name",
     "subQuery"."customers.contact_name" AS "customers.contact_name",
     "subQuery"."customers.contact_title" AS "customers.contact_title",
     "subQuery"."customers.address" AS "customers.address",
     "subQuery"."customers.city" AS "customers.city",
     "subQuery"."customers.region" AS "customers.region",
     "subQuery"."customers.postal_code" AS "customers.postal_code",
     "subQuery"."customers.country" AS "customers.country",
     "subQuery"."customers.phone" AS "customers.phone",
     "subQuery"."customers.fax" AS "customers.fax"
FROM (
          SELECT customers.customer_id AS "customers.customer_id",
               customers.company_name AS "customers.company_name",
               customers.contact_name AS "customers.contact_name",
               customers.contact_title AS "customers.contact_title",
               customers.address AS "customers.address",
               customers.city AS "customers.city",
               customers.region AS "customers.region",
               customers.postal_code AS "customers.postal_code",
               customers.country AS "customers.country",
               customers.phone AS "customers.phone",
               customers.fax AS "customers.fax"
          FROM northwind.customers
     ) AS "subQuery"
ORDER BY "subQuery"."customers.city" ASC, "subQuery"."customers.region" ASC, "subQuery"."customers.postal_code" ASC
LIMIT 3;
`)

	var dest []model.Customers

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	testutils.AssertJSON(t, dest, `
[
	{
		"CustomerID": "DRACD",
		"CompanyName": "Drachenblut Delikatessen",
		"ContactName": "Sven Ottlieb",
		"ContactTitle": "Order Administrator",
		"Address": "Walserweg 21",
		"City": "Aachen",
		"Region": null,
		"PostalCode": "52066",
		"Country": "Germany",
		"Phone": "0241-039123",
		"Fax": "0241-059428"
	},
	{
		"CustomerID": "RATTC",
		"CompanyName": "Rattlesnake Canyon Grocery",
		"ContactName": "Paula Wilson",
		"ContactTitle": "Assistant Sales Representative",
		"Address": "2817 Milton Dr.",
		"City": "Albuquerque",
		"Region": "NM",
		"PostalCode": "87110",
		"Country": "USA",
		"Phone": "(505) 555-5939",
		"Fax": "(505) 555-3620"
	},
	{
		"CustomerID": "OLDWO",
		"CompanyName": "Old World Delicatessen",
		"ContactName": "Rene Phillips",
		"ContactTitle": "Sales Representative",
		"Address": "2743 Bering St.",
		"City": "Anchorage",
		"Region": "AK",
		"PostalCode": "99508",
		"Country": "USA",
		"Phone": "(907) 555-7584",
		"Fax": "(907) 555-2880"
	}
]
`)

}
