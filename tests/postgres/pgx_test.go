package postgres

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/pgxV5"
	. "github.com/go-jet/jet/v2/postgres"
	model3 "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	table3 "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/table"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/northwind/table"
	model2 "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"testing"
)

var pgxConn *pgx.Conn
var pgxPool *pgxpool.Pool

func init() {
	var err error
	pgxConn, err = pgx.Connect(ctx, getConnectionString())

	if err != nil {
		panic(err)
	}

	pgxPool, err = pgxpool.New(ctx, getConnectionString())

	if err != nil {
		panic(err)
	}
}

func BenchmarkNorthwindJoinEverythingPgx(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testNorthwindJoinEverythingCustomScan(b, func(stmt Statement, dest any) {
			err := pgxV5.Query(ctx, stmt, pgxConn, dest)
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
		err := pgxV5.Query(ctx, stmt, pgxConn, dest)
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
	requireLogged(b, stmt)
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
WHERE all_types.uuid = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid;
`, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")

	result := model2.AllTypes{}

	err := pgxV5.Query(ctx, stmt, pgxPool, &result)
	require.NoError(t, err)
	requireLogged(t, stmt)

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

	pgxTx, err := pgxPool.Begin(ctx)
	require.NoError(t, err)
	defer pgxTx.Rollback(ctx)

	err = pgxV5.Query(ctx, query, pgxTx, &result)
	require.NoError(t, err)
	requireLogged(t, query)

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

	err := pgxV5.Query(ctx, stmt, pgxConn, &dest)
	require.NoError(t, err)
	requireLogged(t, stmt)
}

func TestSelectJsonObjectPgxV5(t *testing.T) {
	stmt := SELECT_JSON_OBJ(table3.Actor.AllColumns).
		FROM(table3.Actor).
		WHERE(table3.Actor.ActorID.EQ(Int32(2)))

	var dest model3.Actor

	err := pgxV5.Query(ctx, stmt, pgxPool, &dest)

	require.NoError(t, err)
	testutils.AssertJsonEqual(t, dest, actor2)
	requireLogged(t, stmt)

	t.Run("scan to map", func(t *testing.T) {
		var dest2 map[string]interface{}

		err := pgxV5.Query(ctx, stmt, pgxPool, &dest2)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest2, map[string]interface{}{
			"actorID":    float64(2),
			"firstName":  "Nick",
			"lastName":   "Wahlberg",
			"lastUpdate": "2013-05-26T14:47:57.620000Z",
		})
	})
}

func TestSelectQuickStartJsonPgxV5(t *testing.T) {

	stmt := SELECT_JSON_ARR(
		table3.Actor.ActorID,
		table3.Actor.FirstName,
		table3.Actor.LastName,
		table3.Actor.LastUpdate,

		SELECT_JSON_ARR(
			table3.Film.AllColumns,

			SELECT_JSON_OBJ(
				table3.Language.AllColumns,
			).FROM(
				table3.Language,
			).WHERE(
				table3.Language.LanguageID.EQ(table3.Film.LanguageID).AND(
					table3.Language.Name.EQ(Char(20)("English")),
				),
			).AS("Language"),

			SELECT_JSON_ARR(
				table3.Category.AllColumns,
			).FROM(
				table3.Category.
					INNER_JOIN(table3.FilmCategory, table3.FilmCategory.CategoryID.EQ(table3.Category.CategoryID)),
			).WHERE(
				table3.FilmCategory.FilmID.EQ(table3.Film.FilmID).AND(
					table3.Category.Name.NOT_EQ(Text("Action")),
				),
			).AS("Categories"),
		).FROM(
			table3.Film.
				INNER_JOIN(table3.FilmActor, table3.FilmActor.FilmID.EQ(table3.Film.FilmID)),
		).WHERE(
			AND(
				table3.FilmActor.ActorID.EQ(table3.Actor.ActorID),
				table3.Film.Length.GT(Int32(180)),
				String("Trailers").EQ(ANY(table3.Film.SpecialFeatures)),
			),
		).ORDER_BY(
			table3.Film.FilmID.ASC(),
		).AS("Films"),
	).FROM(
		table3.Actor,
	).ORDER_BY(
		table3.Actor.ActorID.ASC(),
	)

	var dest []struct {
		model3.Actor

		Films []struct {
			model3.Film

			Language   model3.Language
			Categories []model3.Category
		}
	}

	err := pgxV5.Query(ctx, stmt, pgxConn, &dest)

	require.NoError(t, err)
	require.Len(t, dest, 200)
	requireLogged(t, stmt)

	if sourceIsCockroachDB() {
		return // char[n] columns whitespaces are trimmed when returned as json in cockroachdb
	}

	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/quick-start-json-dest.json")
}
