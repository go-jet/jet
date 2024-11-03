package postgres

import (
	"context"
	"github.com/go-jet/jet/v2/qrm"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	model2 "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"

	. "github.com/go-jet/jet/v2/postgres"
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
	require.Equal(t, actor.FirstName, "Nick")
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
		WHERE actor.actor_id IN ($1, $2, $3) AND (($1 / $2) <> ($2 * $3))
		ORDER BY actor.actor_id;
`, int64(1), int64(2), int64(3))

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
		FirstName:  "Nick",
		LastName:   "Wahlberg",
		LastUpdate: *testutils.TimestampWithoutTimeZone("2013-05-26 14:47:57.62", 2),
	})
}

func TestRawInsert(t *testing.T) {
	stmt := RawStatement(`
INSERT INTO test_sample.link (id, url, name, description)
VALUES (@id1, @url1, @name1, DEFAULT),
       (2000, @url1, @name1, NULL),
       (@id2, @url2, @name2, DEFAULT),
	   (@id3, @url3, @name3, NULL)
RETURNING link.id AS "link.id",
         link.url AS "link.url",
         link.name AS "link.name",
         link.description AS "link.description"`,
		RawArgs{
			"@id1": 1000, "@url1": "http://www.postgresqltutorial.com", "@name1": "PostgreSQL Tutorial",
			"@id2": 1010, "@url2": "http://www.google.com", "@name2": "Google",
			"@id3": 1020, "@url3": "http://www.yahoo.com", "@name3": "Yahoo",
		})

	testutils.AssertStatementSql(t, stmt, `
INSERT INTO test_sample.link (id, url, name, description)
VALUES ($1, $2, $3, DEFAULT),
       (2000, $2, $3, NULL),
       ($4, $5, $6, DEFAULT),
	   ($7, $8, $9, NULL)
RETURNING link.id AS "link.id",
         link.url AS "link.url",
         link.name AS "link.name",
         link.description AS "link.description";
`, 1000, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		1010, "http://www.google.com", "Google",
		1020, "http://www.yahoo.com", "Yahoo")

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO test_sample.link (id, url, name, description)
VALUES (1000, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
       (2000, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', NULL),
       (1010, 'http://www.google.com', 'Google', DEFAULT),
	   (1020, 'http://www.yahoo.com', 'Yahoo', NULL)
RETURNING link.id AS "link.id",
         link.url AS "link.url",
         link.name AS "link.name",
         link.description AS "link.description";
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var links []model2.Link
		err := stmt.Query(tx, &links)
		require.NoError(t, err)
		require.Len(t, links, 4)
		require.Equal(t, links[0].ID, int64(1000))
		require.Equal(t, links[1].URL, "http://www.postgresqltutorial.com")
		require.Equal(t, links[2].Name, "Google")
		require.Nil(t, links[2].Description)
	})
}

func TestRawStatementRows(t *testing.T) {
	var stmt Statement

	stmt = RawStatement(`
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

		require.NotEqual(t, actor.ActorID, int32(0))
		require.NotEqual(t, actor.FirstName, "")
		require.NotEqual(t, actor.LastName, "")
		require.NotEqual(t, actor.LastUpdate, time.Time{})

		if actor.ActorID == 54 {
			require.Equal(t, actor.ActorID, int32(54))
			require.Equal(t, actor.FirstName, "Penelope")
			require.Equal(t, actor.LastName, "Pinkett")
			require.Equal(t, actor.LastUpdate.Format(time.RFC3339), "2013-05-26T14:47:57Z")
		}
	}

	err = rows.Close()
	require.NoError(t, err)

	err = rows.Err()
	require.NoError(t, err)

	requireLogged(t, stmt)
}
