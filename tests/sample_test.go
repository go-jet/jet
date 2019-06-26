package tests

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	. "github.com/go-jet/jet"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/table"
	"gotest.tools/assert"
	"testing"
)

func TestUUIDType(t *testing.T) {
	query := AllTypes.
		SELECT(AllTypes.AllColumns).
		WHERE(AllTypes.UUID.EQ(String("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")))

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	assert.Equal(t, len(args), 1)
	fmt.Println(queryStr)
	//assert.Equal(t, queryStr, `SELECT all_types.character AS "all_types.character", all_types.character_varying AS "all_types.character_varying", all_types.text AS "all_types.text", all_types.bytea AS "all_types.bytea", all_types.timestamp_without_time_zone AS "all_types.timestamp_without_time_zone", all_types.timestamp_with_time_zone AS "all_types.timestamp_with_time_zone", all_types.uuid AS "all_types.uuid", all_types.json AS "all_types.json", all_types.jsonb AS "all_types.jsonb" FROM test_sample.all_types WHERE all_types.uuid = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`)
	result := model.AllTypes{}

	err = query.Query(db, &result)
	spew.Dump(result)
}

func TestEnumType(t *testing.T) {
	query := Person.
		SELECT(Person.AllColumns)

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	fmt.Println(queryStr)
	assert.Equal(t, len(args), 0)
	result := []model.Person{}

	err = query.Query(db, &result)

	assert.NilError(t, err)
	//spew.Dump(result)

	type Person struct {
		Name        string
		CurrentMood model.Mood
	}

	result2 := []Person{}

	err = query.Query(db, &result2)

	assert.NilError(t, err)

	spew.Dump(result2)
}

func TestSelecSelfJoin1(t *testing.T) {

	var expectedSql = `
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

	assertStatementSql(t, query, expectedSql)

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
		EmploymentDate: timestampWithTimeZone("1999-01-08 04:05:06.1 +0100 CET", 1),
		ManagerID:      nil,
	})

	assert.Assert(t, dest[0].Manager == nil)

	assert.DeepEqual(t, dest[7].Employee, model.Employee{
		EmployeeID:     8,
		FirstName:      "Salley",
		LastName:       "Lester",
		EmploymentDate: timestampWithTimeZone("1999-01-08 04:05:06 +0100 CET", 1),
		ManagerID:      int32Ptr(3),
	})
}
