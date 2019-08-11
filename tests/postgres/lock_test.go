package postgres

import (
	"context"
	"github.com/go-jet/jet/internal/testutils"
	"gotest.tools/assert"
	"testing"
	"time"

	. "github.com/go-jet/jet/postgres"
	. "github.com/go-jet/jet/tests/.gentestdata/jetdb/dvds/table"
)

func TestLockTable(t *testing.T) {
	expectedSQL := `
LOCK TABLE dvds.address IN`

	var testData = []TableLockMode{
		LOCK_ACCESS_SHARE,
		LOCK_ROW_SHARE,
		LOCK_ROW_EXCLUSIVE,
		LOCK_SHARE_UPDATE_EXCLUSIVE,
		LOCK_SHARE,
		LOCK_SHARE_ROW_EXCLUSIVE,
		LOCK_EXCLUSIVE,
		LOCK_ACCESS_EXCLUSIVE,
	}

	for _, lockMode := range testData {
		query := Address.LOCK().IN(lockMode)

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+string(lockMode)+" MODE;\n")

		tx, _ := db.Begin()

		_, err := query.Exec(tx)

		assert.NilError(t, err)

		err = tx.Rollback()

		assert.NilError(t, err)
	}

	for _, lockMode := range testData {
		query := Address.LOCK().IN(lockMode).NOWAIT()

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+string(lockMode)+" MODE NOWAIT;\n")

		tx, _ := db.Begin()

		_, err := query.Exec(tx)

		assert.NilError(t, err)

		err = tx.Rollback()

		assert.NilError(t, err)
	}
}

func TestLockExecContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	tx, _ := db.Begin()
	defer tx.Rollback()

	_, err := Address.LOCK().IN(LOCK_ACCESS_SHARE).ExecContext(ctx, tx)

	assert.Error(t, err, "context deadline exceeded")
}
