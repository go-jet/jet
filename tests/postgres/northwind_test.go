package postgres

import (
	"context"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/northwind/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/northwind/table"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"testing"
)

var pgxDB *pgx.Conn

func init() {
	var err error
	pgxDB, err = pgx.Connect(context.Background(), getConnectionString())

	if err != nil {
		panic(err)
	}
}

func BenchmarkNorthwindJoinEverythingPGX(b *testing.B) {

	benchQueryScan(b, func(stmt Statement, dest any) {
		sql, args := stmt.Sql()
		_, err := qrm.QueryPGX(context.Background(), pgxDB, sql, args, dest)
		require.NoError(b, err)
	})
}

func BenchmarkNorthwindJoinEverythingPQ(b *testing.B) {

	benchQueryScan(b, func(stmt Statement, dest any) {
		err := stmt.Query(db, dest)
		require.NoError(b, err)
	})
}

func benchQueryScan(b *testing.B, queryFunc func(statement Statement, dest any)) {
	stmt := SELECT(
		Customers.AllColumns,
		CustomerDemographics.AllColumns,
		Orders.AllColumns,
		Shippers.AllColumns,
		OrderDetails.AllColumns,
		Products.AllColumns,
		Categories.AllColumns,
		Suppliers.AllColumns,
	).FROM(
		Customers.
			LEFT_JOIN(CustomerCustomerDemo, Customers.CustomerID.EQ(CustomerCustomerDemo.CustomerID)).
			LEFT_JOIN(CustomerDemographics, CustomerCustomerDemo.CustomerTypeID.EQ(CustomerDemographics.CustomerTypeID)).
			LEFT_JOIN(Orders, Orders.CustomerID.EQ(Customers.CustomerID)).
			LEFT_JOIN(Shippers, Orders.ShipVia.EQ(Shippers.ShipperID)).
			LEFT_JOIN(OrderDetails, Orders.OrderID.EQ(OrderDetails.OrderID)).
			LEFT_JOIN(Products, OrderDetails.ProductID.EQ(Products.ProductID)).
			LEFT_JOIN(Categories, Products.CategoryID.EQ(Categories.CategoryID)).
			LEFT_JOIN(Suppliers, Products.SupplierID.EQ(Suppliers.SupplierID)),
	).ORDER_BY(
		Customers.CustomerID,
		Orders.OrderID,
		Products.ProductID,
	)

	for i := 0; i < b.N; i++ {
		var dest []struct {
			model.Customers

			Demographics model.CustomerDemographics

			Orders []struct {
				model.Orders

				Shipper model.Shippers

				Details struct {
					model.OrderDetails

					Products []struct {
						model.Products

						Category model.Categories
						Supplier model.Suppliers
					}
				}
			}
		}

		queryFunc(stmt, &dest)

		//jsonSave("./testdata/northwind-all.json", dest)
		//testutils.AssertJSONFile(b, dest, "./testdata/results/postgres/northwind-all.json")
		//requireLogged(b, stmt)
	}
}
