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

	RETURNING(projections ...Projection) DeleteStatement
}

func newDeleteStatement(table WritableTable) DeleteStatement {
	return &deleteStatementImpl{
		table: table,
	}
}

type deleteStatementImpl struct {
	table     WritableTable
	where     BoolExpression
	returning []Projection
}

func (d *deleteStatementImpl) WHERE(expression BoolExpression) DeleteStatement {
	d.where = expression
	return d
}

func (d *deleteStatementImpl) RETURNING(projections ...Projection) DeleteStatement {
	d.returning = projections
	return d
}

func (d *deleteStatementImpl) accept(visitor visitor) {
	visitor.visit(d)

	d.table.accept(visitor)
}

func (d *deleteStatementImpl) serializeImpl(out *SqlBuilder) error {
	if d == nil {
		return errors.New("jet: delete statement is nil")
	}
	out.newLine()
	out.WriteString("DELETE FROM")

	if d.table == nil {
		return errors.New("jet: nil tableName")
	}

	if err := d.table.serialize(DeleteStatementType, out); err != nil {
		return err
	}

	if d.where == nil {
		return errors.New("jet: deleting without a WHERE clause")
	}

	if err := out.writeWhere(DeleteStatementType, d.where); err != nil {
		return err
	}

	if err := out.writeReturning(DeleteStatementType, d.returning); err != nil {
		return err
	}

	return nil
}

func (d *deleteStatementImpl) Sql(dialect ...Dialect) (query string, args []interface{}, err error) {
	queryData := &SqlBuilder{
		Dialect: detectDialect(d, dialect...),
	}

	err = d.serializeImpl(queryData)

	if err != nil {
		return
	}

	query, args = queryData.finalize()
	return
}

func (d *deleteStatementImpl) DebugSql(dialect ...Dialect) (query string, err error) {
	return debugSql(d, dialect...)
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
	return execContext(context, d, db)
}
