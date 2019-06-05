package sqlbuilder

import (
	"database/sql"
	"errors"
	"github.com/go-jet/jet/sqlbuilder/execution"
)

type UpdateStatement interface {
	Statement

	SET(values ...interface{}) UpdateStatement
	WHERE(expression BoolExpression) UpdateStatement
	RETURNING(projections ...projection) UpdateStatement
}

func newUpdateStatement(table WritableTable, columns []Column) UpdateStatement {
	return &updateStatementImpl{
		table:   table,
		columns: columns,
	}
}

type updateStatementImpl struct {
	table        WritableTable
	columns      []Column
	updateValues []clause
	where        BoolExpression
	returning    []projection
}

func (u *updateStatementImpl) SET(values ...interface{}) UpdateStatement {

	for _, value := range values {
		if clause, ok := value.(clause); ok {
			u.updateValues = append(u.updateValues, clause)
		} else {
			u.updateValues = append(u.updateValues, literal(value))
		}
	}

	return u
}

func (u *updateStatementImpl) WHERE(expression BoolExpression) UpdateStatement {
	u.where = expression
	return u
}

func (u *updateStatementImpl) RETURNING(projections ...projection) UpdateStatement {
	u.returning = defaultProjectionAliasing(projections)
	return u
}

func (u *updateStatementImpl) Sql() (sql string, args []interface{}, err error) {
	out := &queryData{}

	out.nextLine()
	out.writeString("UPDATE")

	if u.table == nil {
		return "", nil, errors.New("nil tableName.")
	}

	if err = u.table.serialize(update_statement, out); err != nil {
		return
	}

	if len(u.updateValues) == 0 {
		return "", nil, errors.New("No column updated.")
	}

	out.writeString("SET")

	if len(u.columns) > 1 {
		out.writeString("(")
	}

	err = serializeColumnList(update_statement, u.columns, out)

	if err != nil {
		return "", nil, err
	}

	if len(u.columns) > 1 {
		out.writeString(")")
	}

	out.writeString("=")

	if len(u.updateValues) > 1 {
		out.writeString("(")
	}

	for i, value := range u.updateValues {
		if i > 0 {
			out.writeString(", ")
		}

		err = value.serialize(update_statement, out)

		if err != nil {
			return
		}
	}

	if len(u.updateValues) > 1 {
		out.writeString(")")
	}

	if u.where == nil {
		return "", nil, errors.New("Updating without a WHERE clause.")
	}

	if err = out.writeWhere(update_statement, u.where); err != nil {
		return
	}

	if len(u.returning) > 0 {
		out.nextLine()
		out.writeString("RETURNING")

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

func (u *updateStatementImpl) Execute(db execution.Db) (res sql.Result, err error) {
	return Execute(u, db)
}
