package jet

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-jet/jet/execution"
	"github.com/go-jet/jet/internal/utils"
)

// UpdateStatement is interface of SQL UPDATE statement
type UpdateStatement interface {
	Statement

	SET(value interface{}, values ...interface{}) UpdateStatement
	MODEL(data interface{}) UpdateStatement

	WHERE(expression BoolExpression) UpdateStatement
	RETURNING(projections ...Projection) UpdateStatement
}

func newUpdateStatement(table WritableTable, columns []IColumn) UpdateStatement {
	return &updateStatementImpl{
		table:   table,
		columns: columns,
		values:  make([]Clause, 0, len(columns)),
	}
}

type updateStatementImpl struct {
	table     WritableTable
	columns   []IColumn
	values    []Clause
	where     BoolExpression
	returning []Projection
}

func (u *updateStatementImpl) SET(value interface{}, values ...interface{}) UpdateStatement {
	u.values = unwindRowFromValues(value, values)

	return u
}

func (u *updateStatementImpl) MODEL(data interface{}) UpdateStatement {
	u.values = unwindRowFromModel(u.columns, data)

	return u
}

func (u *updateStatementImpl) WHERE(expression BoolExpression) UpdateStatement {
	u.where = expression
	return u
}

func (u *updateStatementImpl) RETURNING(projections ...Projection) UpdateStatement {
	u.returning = projections
	return u
}

func (u *updateStatementImpl) accept(visitor visitor) {
	visitor.visit(u)
	u.table.accept(visitor)
}

func (u *updateStatementImpl) Sql(dialect ...Dialect) (query string, args []interface{}, err error) {
	out := &SqlBuilder{
		Dialect: detectDialect(u, dialect...),
	}

	out.newLine()
	out.WriteString("UPDATE")

	if utils.IsNil(u.table) {
		return "", nil, errors.New("jet: table to update is nil")
	}

	if err = u.table.serialize(UpdateStatementType, out); err != nil {
		return
	}

	if len(u.columns) == 0 {
		return "", nil, errors.New("jet: no columns selected")
	}

	if len(u.values) == 0 {
		return "", nil, errors.New("jet: no values to updated")
	}

	out.newLine()
	out.WriteString("SET")

	if err = out.Dialect.SetClause()(u.columns, u.values, out); err != nil {
		return
	}

	if u.where == nil {
		return "", nil, errors.New("jet: WHERE clause not set")
	}

	if err = out.writeWhere(UpdateStatementType, u.where); err != nil {
		return
	}

	if err = out.writeReturning(UpdateStatementType, u.returning); err != nil {
		return
	}

	query, args = out.finalize()
	return
}

func (u *updateStatementImpl) DebugSql(dialect ...Dialect) (query string, err error) {
	return debugSql(u, dialect...)
}

func (u *updateStatementImpl) Query(db execution.DB, destination interface{}) error {
	return query(u, db, destination)
}

func (u *updateStatementImpl) QueryContext(context context.Context, db execution.DB, destination interface{}) error {
	return queryContext(context, u, db, destination)
}

func (u *updateStatementImpl) Exec(db execution.DB) (res sql.Result, err error) {
	return exec(u, db)
}

func (u *updateStatementImpl) ExecContext(context context.Context, db execution.DB) (res sql.Result, err error) {
	return execContext(context, u, db)
}
