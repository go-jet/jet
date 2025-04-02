package postgres

import (
	"context"
	"fmt"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/northwind/table"
	model2 "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
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
	for i := 0; i < b.N; i++ {
		testNorthwindJoinEverythingCustomScan(b, func(stmt Statement, dest any) {
			sql, args := stmt.Sql()
			_, err := qrm.QueryPGX(context.Background(), pgxDB, sql, args, dest)
			require.NoError(b, err)
		})
	}
}

func BenchmarkNorthwindJoinEverythingPQ(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testNorthwindJoinEverythingCustomScan(b, func(stmt Statement, dest any) {
			err := stmt.Query(db, dest)
			require.NoError(b, err)
		})
	}
}

func TestNorthwindJoinEverythingPQ(t *testing.T) {
	testNorthwindJoinEverythingCustomScan(t, func(stmt Statement, dest any) {
		err := stmt.Query(db, dest)
		require.NoError(t, err)
	})
}

func TestNorthwindJoinEverythingPGX(t *testing.T) {
	testNorthwindJoinEverythingCustomScan(t, func(stmt Statement, dest any) {
		sql, args := stmt.Sql()
		_, err := qrm.QueryPGX(context.Background(), pgxDB, sql, args, dest)
		require.NoError(t, err)
	})
}

func testNorthwindJoinEverythingCustomScan(b require.TestingT, queryFunc func(statement Statement, dest any)) {
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

	var dest Dest

	queryFunc(stmt, &dest)

	testutils.AssertJSONFile(b, dest, "./testdata/results/postgres/northwind-all.json")
}

func TestUUIDTypePGX(t *testing.T) {
	id := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")

	stmt := SELECT(table.AllTypes.UUID, table.AllTypes.UUIDPtr).
		FROM(table.AllTypes).
		WHERE(table.AllTypes.UUID.EQ(UUID(id)))

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT all_types.uuid AS "all_types.uuid",
     all_types.uuid_ptr AS "all_types.uuid_ptr"
FROM test_sample.all_types
WHERE all_types.uuid = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';
`, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")

	result := model2.AllTypes{}

	//err := query.Query(db, &result)

	sql, args := stmt.Sql()
	_, err := qrm.QueryPGX(context.Background(), pgxDB, sql, args, &result)

	require.NoError(t, err)
	require.Equal(t, result.UUID, uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
	testutils.AssertDeepEqual(t, result.UUIDPtr, testutils.UUIDPtr("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
}

func TestPGXScannerType(t *testing.T) {

	type floats struct {
		Numeric    decimal.Decimal
		NumericPtr decimal.Decimal
		Decimal    decimal.Decimal
		DecimalPtr decimal.Decimal
	}

	query := SELECT(
		table.Floats.AllColumns,
	).FROM(
		table.Floats,
	).WHERE(table.Floats.Decimal.EQ(Decimal("1.11111111111111111111")))

	var result floats

	sql, args := query.Sql()
	_, err := qrm.QueryPGX(context.Background(), pgxDB, sql, args, &result)
	require.NoError(t, err)

	require.Equal(t, "1.11111111111111111111", result.Decimal.String())
	require.Equal(t, "0", result.DecimalPtr.String()) // NULL
	require.Equal(t, "2.22222222222222222222", result.Numeric.String())
	require.Equal(t, "0", result.NumericPtr.String()) // NULL
}

func TestAllTypesSelectPGX(t *testing.T) {
	var dest []model2.AllTypes

	stmt := SELECT(table.AllTypes.AllColumns.Except(
		table.AllTypes.Decimal,
		table.AllTypes.DecimalPtr,
		table.AllTypes.Numeric,
		table.AllTypes.NumericPtr,
		table.AllTypes.PointPtr,
		table.AllTypes.Bit,
		table.AllTypes.BitPtr,
		table.AllTypes.BitVarying,
		table.AllTypes.BitVaryingPtr,
		table.AllTypes.JSON,
		table.AllTypes.JSONPtr,
		table.AllTypes.Jsonb,
		table.AllTypes.JsonbPtr,
		table.AllTypes.JsonbArray,
		table.AllTypes.TextArray,
		table.AllTypes.TextArrayPtr,
		table.AllTypes.TextMultiDimArray,
		table.AllTypes.TextMultiDimArrayPtr,
		table.AllTypes.Tsvector,
		table.AllTypes.TsvectorPtr,
		table.AllTypes.IntegerArray,
		table.AllTypes.IntegerArrayPtr,
		table.AllTypes.Interval,
		table.AllTypes.IntervalPtr,

		table.AllTypes.Time,
		table.AllTypes.TimePtr,
		table.AllTypes.Timez,
		table.AllTypes.TimezPtr,
		table.AllTypes.Mood,
		table.AllTypes.MoodPtr,
	)).FROM(
		table.AllTypes,
	).LIMIT(2)

	fmt.Println(stmt.DebugSql())

	sql, args := stmt.Sql()
	_, err := qrm.QueryPGX(context.Background(), pgxDB, sql, args, &dest)

	require.NoError(t, err)
}
