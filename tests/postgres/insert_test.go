package postgres

import (
	"context"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestInsertValues(t *testing.T) {
	insertQuery := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		VALUES(101, "http://www.google.com", "Google", DEFAULT).
		VALUES(102, "http://www.yahoo.com", "Yahoo", nil).
		RETURNING(Link.AllColumns)

	testutils.AssertDebugStatementSql(t, insertQuery, `
INSERT INTO test_sample.link (id, url, name, description)
VALUES (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
       (101, 'http://www.google.com', 'Google', DEFAULT),
       (102, 'http://www.yahoo.com', 'Yahoo', NULL)
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		101, "http://www.google.com", "Google",
		102, "http://www.yahoo.com", "Yahoo", nil)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var insertedLinks []model.Link

		err := insertQuery.Query(tx, &insertedLinks)

		require.NoError(t, err)
		require.Equal(t, len(insertedLinks), 3)
		testutils.AssertDeepEqual(t, insertedLinks[0], model.Link{
			ID:   100,
			URL:  "http://www.postgresqltutorial.com",
			Name: "PostgreSQL Tutorial",
		})
		testutils.AssertDeepEqual(t, insertedLinks[1], model.Link{
			ID:   101,
			URL:  "http://www.google.com",
			Name: "Google",
		})
		testutils.AssertDeepEqual(t, insertedLinks[2], model.Link{
			ID:   102,
			URL:  "http://www.yahoo.com",
			Name: "Yahoo",
		})

		var allLinks []model.Link

		err = Link.SELECT(Link.AllColumns).
			WHERE(Link.ID.BETWEEN(Int(100), Int(199))).
			ORDER_BY(Link.ID).
			Query(tx, &allLinks)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, insertedLinks, allLinks)
	})
}

func TestInsertEmptyColumnList(t *testing.T) {
	stmt := Link.INSERT().
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT)

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO test_sample.link
VALUES (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT);
`,
		100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial")

	testutils.AssertExecAndRollback(t, stmt, db, 1)
	requireLogged(t, stmt)
}

func TestInsertOnConflict(t *testing.T) {
	t.Run("do nothing", func(t *testing.T) {
		employee := model.Employee{EmployeeID: rand.Int31()}

		stmt := Employee.INSERT(Employee.AllColumns).
			MODEL(employee).
			MODEL(employee).
			ON_CONFLICT(Employee.EmployeeID).DO_NOTHING()

		testutils.AssertStatementSql(t, stmt, `
INSERT INTO test_sample.employee (employee_id, first_name, last_name, employment_date, manager_id)
VALUES ($1, $2, $3, $4, $5),
       ($6, $7, $8, $9, $10)
ON CONFLICT (employee_id) DO NOTHING;
`)
		testutils.AssertExecAndRollback(t, stmt, db, 1)
		requireLogged(t, stmt)
	})

	t.Run("on constraint do nothing", func(t *testing.T) {
		skipForCockroachDB(t) // does not support
		employee := model.Employee{EmployeeID: rand.Int31()}

		stmt := Employee.INSERT(Employee.AllColumns).
			MODEL(employee).
			MODEL(employee).
			ON_CONFLICT().ON_CONSTRAINT("employee_pkey").DO_NOTHING()

		testutils.AssertStatementSql(t, stmt, `
INSERT INTO test_sample.employee (employee_id, first_name, last_name, employment_date, manager_id)
VALUES ($1, $2, $3, $4, $5),
       ($6, $7, $8, $9, $10)
ON CONFLICT ON CONSTRAINT employee_pkey DO NOTHING;
`)
		testutils.AssertExecAndRollback(t, stmt, db, 1)
		requireLogged(t, stmt)
	})

	t.Run("do update", func(t *testing.T) {
		stmt := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
			VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
			VALUES(200, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
			ON_CONFLICT(Link.ID).DO_UPDATE(
			SET(
				Link.ID.SET(Link.EXCLUDED.ID),
				Link.URL.SET(String("http://www.postgresqltutorial2.com")),
			),
		).RETURNING(Link.AllColumns)

		testutils.AssertStatementSql(t, stmt, `
INSERT INTO test_sample.link (id, url, name, description)
VALUES ($1, $2, $3, DEFAULT),
       ($4, $5, $6, DEFAULT)
ON CONFLICT (id) DO UPDATE
       SET id = excluded.id,
           url = $7::text
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`)

		testutils.AssertExecAndRollback(t, stmt, db, 2)
	})

	t.Run("on constraint do update", func(t *testing.T) {
		skipForCockroachDB(t) // does not support

		stmt := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
			VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
			VALUES(200, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
			ON_CONFLICT().ON_CONSTRAINT("link_pkey").DO_UPDATE(
			SET(
				Link.ID.SET(Link.EXCLUDED.ID),
				Link.URL.SET(String("http://www.postgresqltutorial2.com")),
			),
		).
			RETURNING(Link.AllColumns)

		testutils.AssertStatementSql(t, stmt, `
INSERT INTO test_sample.link (id, url, name, description)
VALUES ($1, $2, $3, DEFAULT),
       ($4, $5, $6, DEFAULT)
ON CONFLICT ON CONSTRAINT link_pkey DO UPDATE
       SET id = excluded.id,
           url = $7::text
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`)

		testutils.AssertExecAndRollback(t, stmt, db, 2)
	})

	t.Run("do update complex", func(t *testing.T) {
		skipForCockroachDB(t) // does not support ROW

		stmt := Link.INSERT(Link.ID, Link.URL, Link.Name, Link.Description).
			VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
			ON_CONFLICT(Link.ID).WHERE(Link.ID.MUL(Int(2)).GT(Int(10))).DO_UPDATE(
			SET(
				Link.ID.SET(
					IntExp(SELECT(MAXi(Link.ID).ADD(Int(1))).
						FROM(Link)),
				),
				ColumnList{Link.Name, Link.Description}.SET(ROW(Link.EXCLUDED.Name, String("new description"))),
			).WHERE(Link.Description.IS_NOT_NULL()),
		)

		testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO test_sample.link (id, url, name, description)
VALUES (100, 'http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT)
ON CONFLICT (id) WHERE (id * 2) > 10 DO UPDATE
       SET id = (
                SELECT MAX(link.id) + 1
                FROM test_sample.link
           ),
           (name, description) = ROW(excluded.name, 'new description'::text)
       WHERE link.description IS NOT NULL;
`)

		testutils.AssertExecAndRollback(t, stmt, db, 1)
	})
}

func TestInsertModelObject(t *testing.T) {
	linkData := model.Link{
		URL:  "http://www.duckduckgo.com",
		Name: "Duck Duck go",
	}

	query := Link.
		INSERT(Link.URL, Link.Name).
		MODEL(linkData)

	testutils.AssertDebugStatementSql(t, query, `
INSERT INTO test_sample.link (url, name)
VALUES ('http://www.duckduckgo.com', 'Duck Duck go');
`, "http://www.duckduckgo.com", "Duck Duck go")

	testutils.AssertExecAndRollback(t, query, db, 1)
}

func TestInsertModelObjectEmptyColumnList(t *testing.T) {
	linkData := model.Link{
		ID:   1000,
		URL:  "http://www.duckduckgo.com",
		Name: "Duck Duck go",
	}

	query := Link.
		INSERT().
		MODEL(linkData)

	testutils.AssertDebugStatementSql(t, query, `
INSERT INTO test_sample.link
VALUES (1000, 'http://www.duckduckgo.com', 'Duck Duck go', NULL);
`,
		int64(1000), "http://www.duckduckgo.com", "Duck Duck go", nil)

	testutils.AssertExecAndRollback(t, query, db, 1)
}

func TestInsertModelsObject(t *testing.T) {
	tutorial := model.Link{
		URL:  "http://www.postgresqltutorial.com",
		Name: "PostgreSQL Tutorial",
	}

	google := model.Link{
		URL:  "http://www.google.com",
		Name: "Google",
	}

	yahoo := model.Link{
		URL:  "http://www.yahoo.com",
		Name: "Yahoo",
	}

	stmt := Link.
		INSERT(Link.URL, Link.Name).
		MODELS([]model.Link{tutorial, google, yahoo})

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO test_sample.link (url, name)
VALUES ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial'),
       ('http://www.google.com', 'Google'),
       ('http://www.yahoo.com', 'Yahoo');
`,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google",
		"http://www.yahoo.com", "Yahoo")

	testutils.AssertExecAndRollback(t, stmt, db, 3)
}

func TestInsertUsingMutableColumns(t *testing.T) {
	google := model.Link{
		URL:  "http://www.google.com",
		Name: "Google",
	}

	yahoo := model.Link{
		URL:  "http://www.yahoo.com",
		Name: "Yahoo",
	}

	stmt := Link.
		INSERT(Link.MutableColumns).
		VALUES("http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		MODEL(google).
		MODELS([]model.Link{google, yahoo})

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO test_sample.link (url, name, description)
VALUES ('http://www.postgresqltutorial.com', 'PostgreSQL Tutorial', DEFAULT),
       ('http://www.google.com', 'Google', NULL),
       ('http://www.google.com', 'Google', NULL),
       ('http://www.yahoo.com', 'Yahoo', NULL);
`,
		"http://www.postgresqltutorial.com", "PostgreSQL Tutorial",
		"http://www.google.com", "Google", nil,
		"http://www.google.com", "Google", nil,
		"http://www.yahoo.com", "Yahoo", nil)

	testutils.AssertExecAndRollback(t, stmt, db, 4)
}

func TestInsertQuery(t *testing.T) {
	query := Link.
		INSERT(Link.URL, Link.Name).
		QUERY(
			SELECT(Link.URL, Link.Name).
				FROM(Link).
				WHERE(Link.ID.EQ(Int(0))),
		).
		RETURNING(Link.AllColumns)

	testutils.AssertDebugStatementSql(t, query, `
INSERT INTO test_sample.link (url, name) (
     SELECT link.url AS "link.url",
          link.name AS "link.name"
     FROM test_sample.link
     WHERE link.id = 0
)
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`, int64(0))

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var dest []model.Link

		err := query.Query(tx, &dest)
		require.NoError(t, err)

		var youtubeLinks []model.Link
		err = Link.
			SELECT(Link.AllColumns).
			WHERE(Link.Name.EQ(String("Youtube"))).
			Query(tx, &youtubeLinks)

		require.NoError(t, err)
		require.Equal(t, len(youtubeLinks), 2)
	})
}

func TestInsertWithQueryContext(t *testing.T) {
	stmt := Link.INSERT().
		VALUES(1100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT).
		RETURNING(Link.AllColumns)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		dest := []model.Link{}
		err := stmt.QueryContext(ctx, tx, &dest)

		require.Error(t, err, "context deadline exceeded")
	})
}

func TestInsertWithExecContext(t *testing.T) {
	stmt := Link.INSERT().
		VALUES(100, "http://www.postgresqltutorial.com", "PostgreSQL Tutorial", DEFAULT)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		testutils.AssertExecContextErr(ctx, t, stmt, tx, "context deadline exceeded")
	})
}
