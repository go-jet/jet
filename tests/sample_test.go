package tests

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/sub0Zero/go-sqlbuilder/tests/.test_files/dvd_rental/test_sample/model"
	"github.com/sub0Zero/go-sqlbuilder/tests/.test_files/dvd_rental/test_sample/table"
	"gotest.tools/assert"
	"testing"
)

func TestUUIDType(t *testing.T) {
	query := table.AllTypes.
		SELECT(table.AllTypes.AllColumns).
		Where(table.AllTypes.UUID.EqL("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))

	queryStr, err := query.String()

	assert.NilError(t, err)
	fmt.Println(queryStr)
	//assert.Equal(t, queryStr, `SELECT all_types.character AS "all_types.character", all_types.character_varying AS "all_types.character_varying", all_types.text AS "all_types.text", all_types.bytea AS "all_types.bytea", all_types.timestamp_without_time_zone AS "all_types.timestamp_without_time_zone", all_types.timestamp_with_time_zone AS "all_types.timestamp_with_time_zone", all_types.uuid AS "all_types.uuid", all_types.json AS "all_types.json", all_types.jsonb AS "all_types.jsonb" FROM test_sample.all_types WHERE all_types.uuid = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`)
	result := model.AllTypes{}

	err = query.Execute(db, &result)
	spew.Dump(result)
}

func TestEnumType(t *testing.T) {
	query := table.Person.
		SELECT(table.Person.AllColumns)

	queryStr, err := query.String()

	assert.NilError(t, err)
	fmt.Println(queryStr)

	result := []model.Person{}

	err = query.Execute(db, &result)

	assert.NilError(t, err)
	//spew.Dump(result)

	type Person struct {
		Name        string
		CurrentMood model.Mood
	}

	result2 := []Person{}

	err = query.Execute(db, &result2)

	assert.NilError(t, err)

	//spew.Dump(result2)
}
