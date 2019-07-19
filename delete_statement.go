package jet

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-jet/jet/execution"
)

// DeleteStatement is interface for SQL DELETE statement
type DeleteStatement interface {
	Statement

	WHERE(expression BoolExpression) DeleteStatement

	RETURNING(projections ...projection) DeleteStatement
}

func newDeleteStatement(table WritableTable) DeleteStatement {
	return &deleteStatementImpl{
		table: table,
	}
}

type deleteStatementImpl struct {
	table     WritableTable
	where     BoolExpression
	returning []projection
}

func (d *deleteStatementImpl) WHERE(expression BoolExpression) DeleteStatement {
	d.where = expression
	return d
}

func (d *deleteStatementImpl) RETURNING(projections ...projection) DeleteStatement {
	d.returning = projections
	return d
}

func (d *deleteStatementImpl) serializeImpl(out *sqlBuilder) error {
	if d == nil {
		return errors.New("jet: delete statement is nil")
	}
	out.newLine()
	out.writeString("DELETE FROM")

	if d.table == nil {
		return errors.New("jet: nil tableName")
	}

	if err := d.table.serialize(deleteStatement, out); err != nil {
		return err
	}

	if d.where == nil {
		return errors.New("jet: deleting without a WHERE clause")
	}

	if err := out.writeWhere(deleteStatement, d.where); err != nil {
		return err
	}

	if err := out.writeReturning(deleteStatement, d.returning); err != nil {
		return err
	}

	return nil
}

func (d *deleteStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := &sqlBuilder{}

	err = d.serializeImpl(queryData)

	if err != nil {
		return
	}

	query, args = queryData.finalize()
	return
}

func (d *deleteStatementImpl) DebugSql() (query string, err error) {
	return debugSql(d)
}

func (d *deleteStatementImpl) Query(db execution.DB, destination interface{}) error {
	return query(d, db, destination)
}

func (d *deleteStatementImpl) QueryContext(context context.Context, db execution.DB, destination interface{}) error {
	return queryContext(context, d, db, destination)
}

func (d *deleteStatementImpl) Exec(db execution.DB) (res sql.Result, err error) {
	return exec(d, db)
}

func (d *deleteStatementImpl) ExecContext(context context.Context, db execution.DB) (res sql.Result, err error) {
	return execContext(d, db, context)
}
