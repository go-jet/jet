package jet

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-jet/jet/execution"
)

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

func (d *deleteStatementImpl) serializeImpl(out *queryData) error {
	if d == nil {
		return errors.New("delete statement is nil")
	}
	out.newLine()
	out.writeString("DELETE FROM")

	if d.table == nil {
		return errors.New("nil tableName")
	}

	if err := d.table.serialize(delete_statement, out); err != nil {
		return err
	}

	if d.where == nil {
		return errors.New("deleting without a WHERE clause")
	}

	if err := out.writeWhere(delete_statement, d.where); err != nil {
		return err
	}

	if err := out.writeReturning(delete_statement, d.returning); err != nil {
		return err
	}

	return nil
}

func (d *deleteStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := &queryData{}

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

func (d *deleteStatementImpl) QueryContext(db execution.DB, context context.Context, destination interface{}) error {
	return queryContext(d, db, context, destination)
}

func (d *deleteStatementImpl) Exec(db execution.DB) (res sql.Result, err error) {
	return exec(d, db)
}

func (d *deleteStatementImpl) ExecContext(db execution.DB, context context.Context) (res sql.Result, err error) {
	return execContext(d, db, context)
}
