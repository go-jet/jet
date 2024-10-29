package postgres

import (
	"context"
	"database/sql"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/stmtcache"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/table"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPreparedStatementCache(t *testing.T) {
	sqlDB, err := sql.Open("postgres", getConnectionString())
	require.NoError(t, err)
	stmtCachedDB := stmtcache.New(sqlDB)
	defer func(db *stmtcache.DB) {
		err := db.Close()
		require.NoError(t, err)
		require.Equal(t, db.CacheSize(), 0)
	}(stmtCachedDB)
	ctx := context.TODO()

	require.True(t, stmtCachedDB.CachingEnabled())
	require.Equal(t, stmtCachedDB.CacheSize(), 0)

	testStatementCaching := func(cachingEnabled bool) {

		stmtCachedDB.SetCaching(cachingEnabled)
		require.Equal(t, stmtCachedDB.CachingEnabled(), cachingEnabled)

		stmt := Actor.UPDATE().
			SET(Actor.LastName.SET(Actor.LastName)).
			WHERE(Actor.ActorID.BETWEEN(Int(1), Int(10))).
			RETURNING(Actor.AllColumns)

		query, args := stmt.Sql()

		preStmt, err := stmtCachedDB.Prepare(query)
		require.NoError(t, err)

		preStmt2, err := stmtCachedDB.PrepareContext(ctx, query)
		require.NoError(t, err)
		require.Equal(t, preStmt == preStmt2, cachingEnabled)

		t.Run("Exec", func(t *testing.T) {
			testutils.AssertExec(t, stmt, stmtCachedDB, 10)
			testutils.AssertExecContext(t, stmt, ctx, stmtCachedDB, 10)
			_, err := stmtCachedDB.Exec(query, args...)
			require.NoError(t, err)
		})

		t.Run("Query", func(t *testing.T) {
			var dest []model.Actor

			err := stmt.Query(stmtCachedDB, &dest)
			require.NoError(t, err)
			require.Len(t, dest, 10)
			rows, err := stmtCachedDB.Query(query, args...)
			rows.Close()
			require.NoError(t, err)

			t.Run("ctx", func(t *testing.T) {
				var dest []model.Actor
				err := stmt.QueryContext(ctx, stmtCachedDB, &dest)
				require.NoError(t, err)
				require.Len(t, dest, 10)
			})

		})

		t.Run("tx", func(t *testing.T) {
			tx, err := stmtCachedDB.Begin()
			require.NoError(t, err)
			preStmtTx, err := tx.Prepare(query)
			require.NoError(t, err)
			_, err = preStmtTx.Exec(args...)
			require.NoError(t, err)
			preStmtTx2, err := tx.PrepareContext(ctx, query)
			require.NoError(t, err)
			require.Equal(t, preStmtTx == preStmtTx2, cachingEnabled)
			_, err = preStmtTx2.ExecContext(ctx, args...)
			require.NoError(t, err)

			t.Run("Exec", func(t *testing.T) {
				testutils.AssertExec(t, stmt, tx, 10)
				testutils.AssertExecContext(t, stmt, ctx, tx, 10)

				_, err := tx.Exec(query, args...)
				require.NoError(t, err)
			})

			t.Run("Query", func(t *testing.T) {
				var dest []model.Actor
				err = stmt.QueryContext(ctx, tx, &dest)
				require.NoError(t, err)
				require.Len(t, dest, 10)

				rows, err := tx.Query(query, args...)
				require.NoError(t, err)
				require.NoError(t, rows.Close())
			})

			t.Run("new tx", func(t *testing.T) {
				txCtx, err := stmtCachedDB.BeginTx(ctx, nil)
				require.NoError(t, err)

				preStmtTxCtx, err := txCtx.PrepareContext(ctx, query)
				require.NoError(t, err)
				require.NotEqual(t, preStmtTx, preStmtTxCtx)

				require.NoError(t, txCtx.Rollback())
			})

			require.NoError(t, tx.Commit())
		})

		// second prepared statement
		stmt2 := SELECT(Actor.AllColumns).
			FROM(Actor).
			WHERE(Actor.ActorID.EQ(Int(11)))

		var actor model.Actor

		err = stmt2.Query(stmtCachedDB, &actor)
		require.NoError(t, err)
	}

	testStatementCaching(true)
	require.Equal(t, stmtCachedDB.CacheSize(), 2)
	testStatementCaching(false)
	require.Equal(t, stmtCachedDB.CacheSize(), 2)

	// clear all
	require.NoError(t, stmtCachedDB.ClearCache())
	require.Equal(t, stmtCachedDB.CacheSize(), 0)
}
