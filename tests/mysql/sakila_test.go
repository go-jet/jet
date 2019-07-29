package mysql

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/go-jet/jet/internal/testutils"
	. "github.com/go-jet/jet/mysql"
	"gotest.tools/assert"
	"reflect"

	"github.com/go-jet/jet/tests/.gentestdata/sakila/model"
	. "github.com/go-jet/jet/tests/.gentestdata/sakila/table"

	"testing"
)

func TestSelect_ScanToStruct(t *testing.T) {
	expectedSQL := `
SELECT DISTINCT actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update"
FROM sakila.actor
WHERE actor.actor_id = 1;
`
	spew.Dump(reflect.TypeOf(db.Driver()).String())

	query := Actor.
		SELECT(Actor.AllColumns).
		DISTINCT().
		WHERE(Actor.ActorID.EQ(Int(1)))

	testutils.AssertStatementSql(t, query, expectedSQL, int64(1))

	actor := model.Actor{}
	err := query.Query(db, &actor)

	assert.NilError(t, err)

	assert.DeepEqual(t, actor, model.Actor{
		ActorID:    1,
		FirstName:  "PENELOPE",
		LastName:   "GUINESS",
		LastUpdate: *testutils.TimestampWithoutTimeZone("2006-02-15 04:34:33", 2),
	})
}
