package postgres

import (
	"database/sql"
	"github.com/google/uuid"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"

	"github.com/shopspring/decimal"
)

func TestExactDecimals(t *testing.T) {

	type floats struct {
		model.Floats
		Numeric    decimal.Decimal
		NumericPtr decimal.Decimal
		Decimal    decimal.Decimal
		DecimalPtr decimal.Decimal
	}

	t.Run("should query decimal", func(t *testing.T) {
		query := SELECT(
			Floats.AllColumns,
		).FROM(
			Floats,
		).WHERE(Floats.Decimal.EQ(Decimal("1.11111111111111111111")))

		var result floats

		err := query.Query(db, &result)
		require.NoError(t, err)

		require.Equal(t, "1.11111111111111111111", result.Decimal.String())
		require.Equal(t, "0", result.DecimalPtr.String()) // NULL
		require.Equal(t, "2.22222222222222222222", result.Numeric.String())
		require.Equal(t, "0", result.NumericPtr.String()) // NULL

		require.Equal(t, 1.1111111111111112, result.Floats.Decimal) // precision loss
		require.Equal(t, (*float64)(nil), result.Floats.DecimalPtr)
		require.Equal(t, 2.2222222222222223, result.Floats.Numeric) // precision loss
		require.Equal(t, (*float64)(nil), result.Floats.NumericPtr)

		// floating point
		require.Equal(t, float32(3.3333333), result.Floats.Real) // precision loss
		require.Equal(t, (*float32)(nil), result.Floats.RealPtr)
		require.Equal(t, 4.444444444444445, result.Floats.Double) // precision loss
		require.Equal(t, (*float64)(nil), result.Floats.DoublePtr)
	})

	t.Run("should insert decimal", func(t *testing.T) {

		insertQuery := Floats.INSERT(
			Floats.MutableColumns,
		).MODEL(
			floats{
				Floats: model.Floats{
					// overwritten by wrapped(floats) scope
					Numeric:    0.1,
					NumericPtr: testutils.Float64Ptr(0.1),
					Decimal:    0.1,
					DecimalPtr: testutils.Float64Ptr(0.1),

					// not overwritten
					Real:      0.4,
					RealPtr:   testutils.Float32Ptr(0.44),
					Double:    0.3,
					DoublePtr: testutils.Float64Ptr(0.33),
				},
				Numeric:    decimal.RequireFromString("0.1234567890123456789"),
				NumericPtr: decimal.RequireFromString("1.1111111111111111111"),
				Decimal:    decimal.RequireFromString("2.2222222222222222222"),
				DecimalPtr: decimal.RequireFromString("3.3333333333333333333"),
			},
		).RETURNING(
			Floats.MutableColumns,
		)

		testutils.AssertDebugStatementSql(t, insertQuery, `
INSERT INTO test_sample.floats (decimal_ptr, decimal, numeric_ptr, numeric, real_ptr, real, double_ptr, double)
VALUES ('3.3333333333333333333', '2.2222222222222222222', '1.1111111111111111111', '0.1234567890123456789', 0.4399999976158142, 0.4000000059604645, 0.33, 0.3)
RETURNING floats.decimal_ptr AS "floats.decimal_ptr",
          floats.decimal AS "floats.decimal",
          floats.numeric_ptr AS "floats.numeric_ptr",
          floats.numeric AS "floats.numeric",
          floats.real_ptr AS "floats.real_ptr",
          floats.real AS "floats.real",
          floats.double_ptr AS "floats.double_ptr",
          floats.double AS "floats.double";
`)

		var result floats
		err := insertQuery.Query(db, &result)
		require.NoError(t, err)

		// exact decimal
		require.Equal(t, "0.1234567890123456789", result.Numeric.String())
		require.Equal(t, "1.1111111111111111111", result.NumericPtr.String())
		require.Equal(t, "2.2222222222222222222", result.Decimal.String())
		require.Equal(t, "3.3333333333333333333", result.DecimalPtr.String())

		// precision loss
		require.Equal(t, 0.12345678901234568, result.Floats.Numeric)
		require.Equal(t, 1.1111111111111112, *result.Floats.NumericPtr)
		require.Equal(t, 2.2222222222222223, result.Floats.Decimal)
		require.Equal(t, 3.3333333333333335, *result.Floats.DecimalPtr)

		// floating points numbers
		require.Equal(t, float32(0.4), result.Floats.Real)
		require.Equal(t, float32(0.44), *result.Floats.RealPtr)
		require.Equal(t, 0.3, result.Floats.Double)
		require.Equal(t, 0.33, *result.Floats.DoublePtr)
	})
}

func TestUUIDComplex(t *testing.T) {
	query := Person.INNER_JOIN(PersonPhone, PersonPhone.PersonID.EQ(Person.PersonID)).
		SELECT(Person.AllColumns, PersonPhone.AllColumns).
		ORDER_BY(Person.PersonID.ASC(), PersonPhone.PhoneID.ASC())

	t.Run("slice of structs", func(t *testing.T) {

		var dest []struct {
			model.Person
			Phones []struct {
				model.PersonPhone
			}
		}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		require.Equal(t, len(dest), 2)
		testutils.AssertJSON(t, dest, `
[
	{
		"PersonID": "b68dbff4-a87d-11e9-a7f2-98ded00c39c6",
		"FirstName": "Sad",
		"LastName": "John",
		"Mood": "sad",
		"Phones": [
			{
				"PhoneID": "02b61cc4-d500-4847-bd36-111eccbc7a51",
				"PhoneNumber": "212-555-1211",
				"PersonID": "b68dbff4-a87d-11e9-a7f2-98ded00c39c6"
			}
		]
	},
	{
		"PersonID": "b68dbff6-a87d-11e9-a7f2-98ded00c39c8",
		"FirstName": "Ok",
		"LastName": "John",
		"Mood": "ok",
		"Phones": [
			{
				"PhoneID": "02b61cc4-d500-4847-bd36-111eccbc7a52",
				"PhoneNumber": "212-555-1212",
				"PersonID": "b68dbff6-a87d-11e9-a7f2-98ded00c39c8"
			},
			{
				"PhoneID": "02b61cc4-d500-4847-bd36-111eccbc7a53",
				"PhoneNumber": "212-555-1213",
				"PersonID": "b68dbff6-a87d-11e9-a7f2-98ded00c39c8"
			}
		]
	}
]
`)

	})

	t.Run("single struct", func(t *testing.T) {
		uuid, err := uuid.Parse("b68dbff6-a87d-11e9-a7f2-98ded00c39c8")
		require.NoError(t, err)
		singleQuery := query.WHERE(Person.PersonID.EQ(UUID(uuid)))

		var dest struct {
			model.Person
			Phones []struct {
				model.PersonPhone
			}
		}
		err = singleQuery.Query(db, &dest)
		require.NoError(t, err)

		testutils.AssertJSON(t, dest, `
{
	"PersonID": "b68dbff6-a87d-11e9-a7f2-98ded00c39c8",
	"FirstName": "Ok",
	"LastName": "John",
	"Mood": "ok",
	"Phones": [
		{
			"PhoneID": "02b61cc4-d500-4847-bd36-111eccbc7a52",
			"PhoneNumber": "212-555-1212",
			"PersonID": "b68dbff6-a87d-11e9-a7f2-98ded00c39c8"
		},
		{
			"PhoneID": "02b61cc4-d500-4847-bd36-111eccbc7a53",
			"PhoneNumber": "212-555-1213",
			"PersonID": "b68dbff6-a87d-11e9-a7f2-98ded00c39c8"
		}
	]
}
`)
		requireLogged(t, query)
	})

	t.Run("slice of structs left join", func(t *testing.T) {
		leftQuery := Person.LEFT_JOIN(PersonPhone, PersonPhone.PersonID.EQ(Person.PersonID)).
			SELECT(Person.AllColumns, PersonPhone.AllColumns).
			ORDER_BY(Person.PersonID.ASC(), PersonPhone.PhoneID.ASC())
		var dest []struct {
			model.Person
			Phones []struct {
				model.PersonPhone
			}
		}
		err := leftQuery.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertJSON(t, dest, `
[
	{
		"PersonID": "b68dbff4-a87d-11e9-a7f2-98ded00c39c6",
		"FirstName": "Sad",
		"LastName": "John",
		"Mood": "sad",
		"Phones": [
			{
				"PhoneID": "02b61cc4-d500-4847-bd36-111eccbc7a51",
				"PhoneNumber": "212-555-1211",
				"PersonID": "b68dbff4-a87d-11e9-a7f2-98ded00c39c6"
			}
		]
	},
	{
		"PersonID": "b68dbff5-a87d-11e9-a7f2-98ded00c39c7",
		"FirstName": "Ok",
		"LastName": "John",
		"Mood": "ok",
		"Phones": null
	},
	{
		"PersonID": "b68dbff6-a87d-11e9-a7f2-98ded00c39c8",
		"FirstName": "Ok",
		"LastName": "John",
		"Mood": "ok",
		"Phones": [
			{
				"PhoneID": "02b61cc4-d500-4847-bd36-111eccbc7a52",
				"PhoneNumber": "212-555-1212",
				"PersonID": "b68dbff6-a87d-11e9-a7f2-98ded00c39c8"
			},
			{
				"PhoneID": "02b61cc4-d500-4847-bd36-111eccbc7a53",
				"PhoneNumber": "212-555-1213",
				"PersonID": "b68dbff6-a87d-11e9-a7f2-98ded00c39c8"
			}
		]
	}
]
`)
		requireLogged(t, leftQuery)
	})

}
func TestEnumType(t *testing.T) {
	query := Person.
		SELECT(Person.AllColumns)

	testutils.AssertDebugStatementSql(t, query, `
SELECT person.person_id AS "person.person_id",
     person.first_name AS "person.first_name",
     person.last_name AS "person.last_name",
     person."Mood" AS "person.Mood"
FROM test_sample.person;
`)

	var result []model.Person

	err := query.Query(db, &result)

	require.NoError(t, err)
	testutils.AssertJSON(t, result, `
[
	{
		"PersonID": "b68dbff4-a87d-11e9-a7f2-98ded00c39c6",
		"FirstName": "Sad",
		"LastName": "John",
		"Mood": "sad"
	},
	{
		"PersonID": "b68dbff5-a87d-11e9-a7f2-98ded00c39c7",
		"FirstName": "Ok",
		"LastName": "John",
		"Mood": "ok"
	},
	{
		"PersonID": "b68dbff6-a87d-11e9-a7f2-98ded00c39c8",
		"FirstName": "Ok",
		"LastName": "John",
		"Mood": "ok"
	}
]
`)
}

func TestSelectSelfJoin1(t *testing.T) {

	// clean up
	_, err := Employee.DELETE().WHERE(Employee.EmployeeID.GT(Int(100))).Exec(db)
	require.NoError(t, err)

	var expectedSQL = `
SELECT employee.employee_id AS "employee.employee_id",
     employee.first_name AS "employee.first_name",
     employee.last_name AS "employee.last_name",
     employee.employment_date AS "employee.employment_date",
     employee.manager_id AS "employee.manager_id",
     manager.employee_id AS "manager.employee_id",
     manager.first_name AS "manager.first_name",
     manager.last_name AS "manager.last_name",
     manager.employment_date AS "manager.employment_date",
     manager.manager_id AS "manager.manager_id"
FROM test_sample.employee
     LEFT JOIN test_sample.employee AS manager ON (manager.employee_id = employee.manager_id)
ORDER BY employee.employee_id;
`

	manager := Employee.AS("manager")
	query := Employee.
		LEFT_JOIN(manager, manager.EmployeeID.EQ(Employee.ManagerID)).
		SELECT(
			Employee.AllColumns,
			manager.AllColumns,
		).
		ORDER_BY(Employee.EmployeeID)

	testutils.AssertDebugStatementSql(t, query, expectedSQL)

	type Manager model.Employee

	var dest []struct {
		model.Employee

		Manager *Manager
	}

	err = query.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 8)
	testutils.AssertDeepEqual(t, dest[0].Employee, model.Employee{
		EmployeeID:     1,
		FirstName:      "Windy",
		LastName:       "Hays",
		EmploymentDate: testutils.TimestampWithTimeZone("1999-01-08 04:05:06.1 +0100 CET", 1),
		ManagerID:      nil,
	})

	require.True(t, dest[0].Manager == nil)

	testutils.AssertDeepEqual(t, dest[7].Employee, model.Employee{
		EmployeeID:     8,
		FirstName:      "Salley",
		LastName:       "Lester",
		EmploymentDate: testutils.TimestampWithTimeZone("1999-01-08 04:05:06 +0100 CET", 1),
		ManagerID:      testutils.Int32Ptr(3),
	})
}

func TestWierdNamesTable(t *testing.T) {
	stmt := WeirdNamesTable.SELECT(WeirdNamesTable.MutableColumns)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "WEIRD NAMES TABLE".weird_column_name1 AS "WEIRD NAMES TABLE.weird_column_name1",
     "WEIRD NAMES TABLE"."Weird_Column_Name2" AS "WEIRD NAMES TABLE.Weird_Column_Name2",
     "WEIRD NAMES TABLE"."wEiRd_cOluMn_nAmE3" AS "WEIRD NAMES TABLE.wEiRd_cOluMn_nAmE3",
     "WEIRD NAMES TABLE"."WeIrd_CoLuMN_Name4" AS "WEIRD NAMES TABLE.WeIrd_CoLuMN_Name4",
     "WEIRD NAMES TABLE"."WEIRD_COLUMN_NAME5" AS "WEIRD NAMES TABLE.WEIRD_COLUMN_NAME5",
     "WEIRD NAMES TABLE"."WeirdColumnName6" AS "WEIRD NAMES TABLE.WeirdColumnName6",
     "WEIRD NAMES TABLE"."weirdColumnName7" AS "WEIRD NAMES TABLE.weirdColumnName7",
     "WEIRD NAMES TABLE".weirdcolumnname8 AS "WEIRD NAMES TABLE.weirdcolumnname8",
     "WEIRD NAMES TABLE"."weird col name9" AS "WEIRD NAMES TABLE.weird col name9",
     "WEIRD NAMES TABLE"."wEiRd cOlu nAmE10" AS "WEIRD NAMES TABLE.wEiRd cOlu nAmE10",
     "WEIRD NAMES TABLE"."WEIRD COLU NAME11" AS "WEIRD NAMES TABLE.WEIRD COLU NAME11",
     "WEIRD NAMES TABLE"."Weird Colu Name12" AS "WEIRD NAMES TABLE.Weird Colu Name12",
     "WEIRD NAMES TABLE"."weird-col-name13" AS "WEIRD NAMES TABLE.weird-col-name13",
     "WEIRD NAMES TABLE"."wEiRd-cOlu-nAmE14" AS "WEIRD NAMES TABLE.wEiRd-cOlu-nAmE14",
     "WEIRD NAMES TABLE"."WEIRD-COLU-NAME15" AS "WEIRD NAMES TABLE.WEIRD-COLU-NAME15",
     "WEIRD NAMES TABLE"."Weird-Colu-Name16" AS "WEIRD NAMES TABLE.Weird-Colu-Name16"
FROM test_sample."WEIRD NAMES TABLE";
`)

	var dest []model.WeirdNamesTable

	err := stmt.Query(db, &dest)

	require.NoError(t, err)

	require.Equal(t, len(dest), 1)
	testutils.AssertDeepEqual(t, dest[0], model.WeirdNamesTable{
		WeirdColumnName1: "Doe",
		WeirdColumnName2: "Doe",
		WeirdColumnName3: "Doe",
		WeirdColumnName4: "Doe",
		WeirdColumnName5: "Doe",
		WeirdColumnName6: "Doe",
		WeirdColumnName7: "Doe",
		Weirdcolumnname8: testutils.StringPtr("Doe"),
		WeirdColName9:    "Doe",
		WeirdColuName10:  "Doe",
		WeirdColuName11:  "Doe",
		WeirdColuName12:  "Doe",
		WeirdColName13:   "Doe",
		WeirdColuName14:  "Doe",
		WeirdColuName15:  "Doe",
		WeirdColuName16:  "Doe",
	})
}

func TestReserwedWordEscape(t *testing.T) {
	stmt := SELECT(User.MutableColumns).
		FROM(User)

	//fmt.Println(stmt.DebugSql())

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "User"."column" AS "User.column",
     "User"."check" AS "User.check",
     "User".ceil AS "User.ceil",
     "User".commit AS "User.commit",
     "User"."create" AS "User.create",
     "User"."default" AS "User.default",
     "User"."desc" AS "User.desc",
     "User".empty AS "User.empty",
     "User".float AS "User.float",
     "User".join AS "User.join",
     "User".like AS "User.like",
     "User".max AS "User.max",
     "User".rank AS "User.rank"
FROM test_sample."User";
`)

	var dest []model.User

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	//testutils.PrintJson(dest)

	testutils.AssertJSON(t, dest, `
[
	{
		"ID": 0,
		"Column": "Column",
		"Check": "CHECK",
		"Ceil": "CEIL",
		"Commit": "COMMIT",
		"Create": "CREATE",
		"Default": "DEFAULT",
		"Desc": "DESC",
		"Empty": "EMPTY",
		"Float": "FLOAT",
		"Join": "JOIN",
		"Like": "LIKE",
		"Max": "MAX",
		"Rank": "RANK"
	}
]
`)
}

func TestMutableColumnsExcludeGeneratedColumn(t *testing.T) {

	t.Run("should not have the generated column in mutableColumns", func(t *testing.T) {
		require.Equal(t, 2, len(People.MutableColumns))
		require.Equal(t, People.PeopleName, People.MutableColumns[0])
		require.Equal(t, People.PeopleHeightCm, People.MutableColumns[1])
	})

	t.Run("should query with all columns", func(t *testing.T) {
		query := SELECT(
			People.AllColumns,
		).FROM(
			People,
		).WHERE(
			People.PeopleID.EQ(Int(3)),
		)

		var result model.People

		err := query.Query(db, &result)
		require.NoError(t, err)

		require.Equal(t, "Carla", result.PeopleName)
		require.Equal(t, 155., *result.PeopleHeightCm)
		require.InEpsilon(t, 61.02, *result.PeopleHeightIn, 1e-3)
	})

	t.Run("should insert without generated columns", func(t *testing.T) {
		testutils.ExecuteInTxAndRollback(t, db, func(tx *sql.Tx) {
			insertQuery := People.INSERT(
				People.MutableColumns,
			).MODEL(
				model.People{
					PeopleName:     "Dario",
					PeopleHeightCm: testutils.Float64Ptr(120),
				},
			).RETURNING(
				People.MutableColumns,
			)

			testutils.AssertDebugStatementSql(t, insertQuery, `
INSERT INTO test_sample.people (people_name, people_height_cm)
VALUES ('Dario', 120)
RETURNING people.people_name AS "people.people_name",
          people.people_height_cm AS "people.people_height_cm";
`)
			var result model.People
			err := insertQuery.Query(tx, &result)
			require.NoError(t, err)

			require.Equal(t, "Dario", result.PeopleName)
			require.Equal(t, 120., *result.PeopleHeightCm)

			query := SELECT(
				People.AllColumns,
			).FROM(
				People,
			).ORDER_BY(
				People.PeopleID.DESC(),
			).LIMIT(1)

			result = model.People{}

			err = query.Query(tx, &result)
			require.NoError(t, err)

			require.Equal(t, "Dario", result.PeopleName)
			require.Equal(t, 120., *result.PeopleHeightCm)
			require.InEpsilon(t, 47.24, *result.PeopleHeightIn, 1e-3)
		})
	})
}
