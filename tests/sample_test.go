package tests

import (
	. "github.com/go-jet/jet"
	"github.com/go-jet/jet/internal/testutils"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/table"
	"github.com/google/uuid"
	"gotest.tools/assert"
	"testing"
)

func TestUUIDType(t *testing.T) {
	query := AllTypes.
		SELECT(AllTypes.UUID, AllTypes.UUIDPtr).
		WHERE(AllTypes.UUID.EQ(String("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")))

	testutils.AssertStatementSql(t, query, `
SELECT all_types.uuid AS "all_types.uuid",
     all_types.uuid_ptr AS "all_types.uuid_ptr"
FROM test_sample.all_types
WHERE all_types.uuid = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';
`, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")

	result := model.AllTypes{}

	err := query.Query(db, &result)
	assert.NilError(t, err)
	assert.Equal(t, result.UUID, uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
	assert.DeepEqual(t, result.UUIDPtr, UUIDPtr("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
}

func TestEnumType(t *testing.T) {
	query := Person.
		SELECT(Person.AllColumns)

	testutils.AssertStatementSql(t, query, `
SELECT person.person_id AS "person.person_id",
     person.first_name AS "person.first_name",
     person.last_name AS "person.last_name",
     person."Mood" AS "person.Mood"
FROM test_sample.person;
`)

	result := []model.Person{}

	err := query.Query(db, &result)

	assert.NilError(t, err)
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

func TestSelecSelfJoin1(t *testing.T) {

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

	testutils.AssertStatementSql(t, query, expectedSQL)

	type Manager model.Employee

	var dest []struct {
		model.Employee

		Manager *Manager
	}

	err := query.Query(db, &dest)

	assert.NilError(t, err)
	assert.Equal(t, len(dest), 8)
	assert.DeepEqual(t, dest[0].Employee, model.Employee{
		EmployeeID:     1,
		FirstName:      "Windy",
		LastName:       "Hays",
		EmploymentDate: TimestampWithTimeZone("1999-01-08 04:05:06.1 +0100 CET", 1),
		ManagerID:      nil,
	})

	assert.Assert(t, dest[0].Manager == nil)

	assert.DeepEqual(t, dest[7].Employee, model.Employee{
		EmployeeID:     8,
		FirstName:      "Salley",
		LastName:       "Lester",
		EmploymentDate: TimestampWithTimeZone("1999-01-08 04:05:06 +0100 CET", 1),
		ManagerID:      Int32Ptr(3),
	})
}

func TestWierdNamesTable(t *testing.T) {
	stmt := WeirdNamesTable.SELECT(WeirdNamesTable.AllColumns)

	testutils.AssertStatementSql(t, stmt, `
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

	dest := []model.WeirdNamesTable{}

	err := stmt.Query(db, &dest)

	assert.NilError(t, err)

	assert.Equal(t, len(dest), 1)
	assert.DeepEqual(t, dest[0], model.WeirdNamesTable{
		WeirdColumnName1: "Doe",
		WeirdColumnName2: "Doe",
		WeirdColumnName3: "Doe",
		WeirdColumnName4: "Doe",
		WeirdColumnName5: "Doe",
		WeirdColumnName6: "Doe",
		WeirdColumnName7: "Doe",
		Weirdcolumnname8: StringPtr("Doe"),
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
