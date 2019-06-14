package sqlbuilder

import (
	"database/sql"
	"errors"
	"github.com/go-jet/jet/sqlbuilder/execution"
)

type UpdateStatement interface {
	Statement

	SET(value interface{}, values ...interface{}) UpdateStatement
	USING(data interface{}) UpdateStatement

	WHERE(expression BoolExpression) UpdateStatement
	RETURNING(projections ...projection) UpdateStatement
}

func newUpdateStatement(table WritableTable, columns []column) UpdateStatement {
	return &updateStatementImpl{
		table:   table,
		columns: columns,
		row:     make([]clause, 0, len(columns)),
	}
}

type updateStatementImpl struct {
	table     WritableTable
	columns   []column
	row       []clause
	where     BoolExpression
	returning []projection
}

func (u *updateStatementImpl) SET(value interface{}, values ...interface{}) UpdateStatement {
	u.row = unwindRowFromValues(value, values)

	return u
}

func (u *updateStatementImpl) USING(modelData interface{}) UpdateStatement {
	u.row = unwindRowFromModel(u.columns, modelData)

	return u
}

func (u *updateStatementImpl) WHERE(expression BoolExpression) UpdateStatement {
	u.where = expression
	return u
}

func (u *updateStatementImpl) RETURNING(projections ...projection) UpdateStatement {
	u.returning = projections
	return u
}

func (u *updateStatementImpl) Sql() (sql string, args []interface{}, err error) {
	out := &queryData{}

	out.newLine()
	out.writeString("UPDATE")

	if isNil(u.table) {
		return "", nil, errors.New("table to update is nil")
	}

	if err = u.table.serialize(update_statement, out); err != nil {
		return
	}

	if len(u.columns) == 0 {
		return "", nil, errors.New("no columns selected")
	}

	if len(u.row) == 0 {
		return "", nil, errors.New("no values to updated")
	}

	out.newLine()
	out.writeString("SET")

	if len(u.columns) > 1 {
		out.writeString("(")
	}

	err = serializeColumnNames(u.columns, out)

	if err != nil {
		return
	}

	if len(u.columns) > 1 {
		out.writeString(")")
	}

	out.writeString("=")

	if len(u.row) > 1 {
		out.writeString("(")
	}

	err = serializeClauseList(update_statement, u.row, out)

	if err != nil {
		return
	}

	if len(u.row) > 1 {
		out.writeString(")")
	}

	if u.where == nil {
		return "", nil, errors.New("WHERE clause not set")
	}

	if err = out.writeWhere(update_statement, u.where); err != nil {
		return
	}

	if len(u.returning) > 0 {
		out.newLine()
		out.writeString("RETURNING")
		out.increaseIdent()
		out.increaseIdent()

		err = serializeProjectionList(update_statement, u.returning, out)

		if err != nil {
			return
		}
	}

	sql, args = out.finalize()
	return
}

func (u *updateStatementImpl) DebugSql() (query string, err error) {
	return DebugSql(u)
}

func (u *updateStatementImpl) Query(db execution.Db, destination interface{}) error {
	return Query(u, db, destination)
}

func (u *updateStatementImpl) Exec(db execution.Db) (res sql.Result, err error) {
	return Exec(u, db)
}
