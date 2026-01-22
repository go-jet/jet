package pgxV5

import (
	"context"
	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/jackc/pgx/v5/pgconn"
)

func Query(ctx context.Context, statement postgres.Statement, pgx qrm.QueryablePgxV5, dest any) error {
	return jet.QueryWithLogging(ctx, statement, func(query string, args []interface{}) (int64, error) {
		switch statement.Type() {
		case jet.SelectJsonObjStatementType:
			return qrm.QueryJsonObjPgxV5(ctx, pgx, query, args, dest)
		case jet.SelectJsonArrStatementType:
			return qrm.QueryJsonArrPgxV5(ctx, pgx, query, args, dest)
		default:
			return qrm.QueryPgxV5(ctx, pgx, query, args, dest)
		}
	})
}

func Exec(ctx context.Context, statement postgres.Statement, pgx qrm.ExecutablePgxV5) (result pgconn.CommandTag, err error) {
	err = jet.QueryWithLogging(ctx, statement, func(query string, args []interface{}) (int64, error) {
		var execErr error
		result, execErr = pgx.Exec(ctx, query, args...)

		if execErr != nil {
			return 0, execErr
		}

		return result.RowsAffected(), nil
	})

	return
}
