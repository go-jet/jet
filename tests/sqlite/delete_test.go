package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/table"
	"github.com/stretchr/testify/require"
)

func TestDelete_WHERE_RETURNING(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	var expectedSQL = `
DELETE FROM link
WHERE link.name IN ('Bing', 'Yahoo')
RETURNING link.id AS "link.id",
          link.url AS "link.url",
          link.name AS "link.name",
          link.description AS "link.description";
`
	deleteStmt := Link.DELETE().
		WHERE(Link.Name.IN(String("Bing"), String("Yahoo"))).
		RETURNING(Link.AllColumns)

	testutils.AssertDebugStatementSql(t, deleteStmt, expectedSQL, "Bing", "Yahoo")
	var dest []model.Link
	err := deleteStmt.Query(tx, &dest)
	require.NoError(t, err)
	require.Len(t, dest, 2)
	requireLogged(t, deleteStmt)
}

func TestDeleteWithWhereOrderByLimit(t *testing.T) {
	t.SkipNow() // Until https://github.com/mattn/go-sqlite3/pull/802 is fixed
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	sampleDB.Stats()

	var expectedSQL = `
DELETE FROM link
WHERE link.name IN ('Bing', 'Yahoo')
ORDER BY link.name
LIMIT 1;
`
	deleteStmt := Link.DELETE().
		WHERE(Link.Name.IN(String("Bing"), String("Yahoo"))).
		ORDER_BY(Link.Name).
		LIMIT(1)

	testutils.AssertDebugStatementSql(t, deleteStmt, expectedSQL, "Bing", "Yahoo", int64(1))
	testutils.AssertExec(t, deleteStmt, tx, 1)
	requireLogged(t, deleteStmt)
}

func TestDeleteContextDeadlineExceeded(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	deleteStmt := Link.
		DELETE().
		WHERE(Link.Name.IN(String("Bing"), String("Yahoo")))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	dest := []model.Link{}
	err := deleteStmt.QueryContext(ctx, tx, &dest)
	require.Error(t, err, "context deadline exceeded")

	_, err = deleteStmt.ExecContext(ctx, tx)
	require.Error(t, err, "context deadline exceeded")

	requireLogged(t, deleteStmt)
}

func TestDeletePreparedStatement(t *testing.T) {
	ctx := context.Background()

	t.Run("tx prep stmt", func(t *testing.T) {
		var txPrepStmt PreparedStatement
		defer txPrepStmt.Close()

		tx, err := sampleDB.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		for i := 20; i < 24; i++ {
			stmt := Link.DELETE().
				WHERE(Link.ID.EQ(Int64(int64(i))))

			err = txPrepStmt.Prepare(ctx, tx, stmt)
			require.NoError(t, err)

			res, err := txPrepStmt.Exec(ctx)
			require.NoError(t, err)
			rowsAffected, err := res.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, rowsAffected, int64(1))
		}
	})

	t.Run("db tx prep stmt", func(t *testing.T) {
		var dbTxPrepStmt PreparedStatement
		defer dbTxPrepStmt.Close()

		tx, err := sampleDB.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		var dest model.Link

		for i := 20; i < 24; i++ {
			stmt := Link.DELETE().
				WHERE(Link.ID.EQ(Int64(int64(i)))).
				RETURNING(Link.AllColumns)

			err = dbTxPrepStmt.Prepare(ctx, sampleDB, stmt)
			require.NoError(t, err)
			err = dbTxPrepStmt.Stmt(tx).Query(ctx, &dest)
			require.NoError(t, err)
			require.NotEmpty(t, dest)
		}
	})

	t.Run("rows prep stmt", func(t *testing.T) {
		var prepStmt PreparedStatement
		prepStmt.Close()

		tx, err := sampleDB.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		var dest model.Link

		for i := 20; i < 24; i++ {
			stmt := Link.DELETE().
				WHERE(Link.ID.EQ(Int64(int64(i)))).
				RETURNING(Link.AllColumns)

			err = prepStmt.Prepare(ctx, tx, stmt)
			require.NoError(t, err)
			rows, err := prepStmt.Rows(ctx)
			require.NoError(t, err)

			require.True(t, rows.Next())
			err = rows.Scan(&dest)
			require.NoError(t, err)
			require.NotEmpty(t, dest)
			require.NoError(t, rows.Close())
		}
	})
}
