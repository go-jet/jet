package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	model2 "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/model"

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
	cleanUpLinkTable(t)

	stmt := RawStatement(`
INSERT INTO test_sample.link (id, url, name, description)
VALUES (@id1, @url1, @name1, DEFAULT),
       (200, @url1, @name1, NULL),
       (@id2, @url2, @name2, DEFAULT),
	   (@id3, @url3, @name3, NULL)
RETURNING link.id AS "link.id",
         link.url AS "link.url",
         link.name AS "link.name",
         link.description AS "link.description"`,
		RawArgs{
			"@id1": 100, "@url1": "http://www.postgresqltutorial.com", "@name1": "PostgreSQL Tutorial",
			"@id2": 101, "@url2": "http://www.google.com", "@name2": "Google",
			"@id3": 102, "@url3": "http://www.yahoo.com", "@name3": "Yahoo",
		})

	testutils.AssertStatementSql(t, stmt, `
INSERT INTO test_sample.link (id, url, name, description)
VALUES ($1, $2, $3, DEFAULT),
       (200, $2, $3, NULL),
       ($4, $5, $6, DEFAULT),
	   ($7, $8, $9, NULL)
RETURNING link.id AS "link.id",
         link.url AS "link.url",
         link.name AS "link.name",
         link.description AS "link.description";
`, 100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		101, "http://www.google.com", "Google",
		102, "http://www.yahoo.com", "Yahoo")

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO test_sample.link (id, url, name, description)
VALUES (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
       (200, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', NULL),
       (101, 'http://www.google.com', 'Google', DEFAULT),
	   (102, 'http://www.yahoo.com', 'Yahoo', NULL)
RETURNING link.id AS "link.id",
         link.url AS "link.url",
         link.name AS "link.name",
         link.description AS "link.description";
`)

	var links []model2.Link
	err := stmt.Query(db, &links)
	require.NoError(t, err)
	require.Len(t, links, 4)
	require.Equal(t, links[0].ID, int32(100))
	require.Equal(t, links[1].URL, "http://www.postgresqltutorial.com")
	require.Equal(t, links[2].Name, "Google")
	require.Nil(t, links[2].Description)
}
