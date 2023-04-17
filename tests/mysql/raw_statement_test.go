package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/model"

	. "github.com/go-jet/jet/v2/mysql"
)

func TestRawStatementSelect(t *testing.T) {
	stmt := RawStatement(`
		SELECT actor.first_name AS "actor.first_name"
		FROM dvds.actor
		WHERE actor.actor_id = 2`)

	testutils.AssertStatementSql(t, stmt, `
		SELECT actor.first_name AS "actor.first_name"
		FROM dvds.actor
		WHERE actor.actor_id = 2;
`)
	testutils.AssertDebugStatementSql(t, stmt, `
		SELECT actor.first_name AS "actor.first_name"
		FROM dvds.actor
		WHERE actor.actor_id = 2;
`)
	var actor model.Actor
	err := stmt.Query(db, &actor)
	require.NoError(t, err)
	require.Equal(t, actor.FirstName, "NICK")
}

func TestRawStatementSelectWithArguments(t *testing.T) {
	stmt := RawStatement(`
		SELECT DISTINCT actor.actor_id AS "actor.actor_id",
			 actor.first_name AS "actor.first_name",
			 actor.last_name AS "actor.last_name",
			 actor.last_update AS "actor.last_update"
		FROM dvds.actor
		WHERE actor.actor_id IN (#actorID1, #actorID2, #actorID3) AND ((#actorID1 / #actorID2) <> (#actorID2 * #actorID3))
		ORDER BY actor.actor_id`,
		RawArgs{
			"#actorID1": int64(1),
			"#actorID2": int64(2),
			"#actorID3": int64(3),
		},
	)

	testutils.AssertStatementSql(t, stmt, `
		SELECT DISTINCT actor.actor_id AS "actor.actor_id",
			 actor.first_name AS "actor.first_name",
			 actor.last_name AS "actor.last_name",
			 actor.last_update AS "actor.last_update"
		FROM dvds.actor
		WHERE actor.actor_id IN (?, ?, ?) AND ((? / ?) <> (? * ?))
		ORDER BY actor.actor_id;
`, int64(1), int64(2), int64(3), int64(1), int64(2), int64(2), int64(3))

	testutils.AssertDebugStatementSql(t, stmt, `
		SELECT DISTINCT actor.actor_id AS "actor.actor_id",
			 actor.first_name AS "actor.first_name",
			 actor.last_name AS "actor.last_name",
			 actor.last_update AS "actor.last_update"
		FROM dvds.actor
		WHERE actor.actor_id IN (1, 2, 3) AND ((1 / 2) <> (2 * 3))
		ORDER BY actor.actor_id;
`)

	var actor []model.Actor
	err := stmt.Query(db, &actor)
	require.NoError(t, err)

	testutils.AssertDeepEqual(t, actor[1], model.Actor{
		ActorID:    2,
		FirstName:  "NICK",
		LastName:   "WAHLBERG",
		LastUpdate: *testutils.TimestampWithoutTimeZone("2006-02-15 04:34:33", 2),
	})
}

func TestRawStatementRows(t *testing.T) {
	stmt := RawStatement(`
		SELECT actor.actor_id AS "actor.actor_id",
			 actor.first_name AS "actor.first_name",
			 actor.last_name AS "actor.last_name",
			 actor.last_update AS "actor.last_update"
		FROM dvds.actor
		ORDER BY actor.actor_id`)

	var rows *Rows
	var err error

	rows, err = stmt.Rows(context.Background(), db)
	require.NoError(t, err)

	for rows.Next() {
		var actor model.Actor
		err := rows.Scan(&actor)
		require.NoError(t, err)

		require.NotEqual(t, actor.ActorID, int16(0))
		require.NotEqual(t, actor.FirstName, "")
		require.NotEqual(t, actor.LastName, "")
		require.NotEqual(t, actor.LastUpdate, time.Time{})

		if actor.ActorID == 54 {
			require.Equal(t, actor.ActorID, uint16(54))
			require.Equal(t, actor.FirstName, "PENELOPE")
			require.Equal(t, actor.LastName, "PINKETT")
			require.Equal(t, actor.LastUpdate.Format(time.RFC3339), "2006-02-15T04:34:33Z")
		}
	}

	err = rows.Close()
	require.NoError(t, err)

	err = rows.Err()
	require.NoError(t, err)

	requireLogged(t, stmt)
}
