package sqlbuilder

import (
	"bytes"
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/sqlbuilder/execution"
	"github.com/sub0zero/go-sqlbuilder/types"
)

type UpdateStatement interface {
	Statement

	SET(values ...interface{}) UpdateStatement
	WHERE(expression BoolExpression) UpdateStatement
	RETURNING(projections ...Projection) UpdateStatement

	Query(db types.Db, destination interface{}) error
	Execute(db types.Db) (sql.Result, error)
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
	updateValues []Clause
	where        BoolExpression
	returning    []Projection
}

func (u *updateStatementImpl) Query(db types.Db, destination interface{}) error {
	query, err := u.String()

	if err != nil {
		return err
	}

	return execution.Execute(db, query, destination)
}

func (u *updateStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
	query, err := u.String()

	if err != nil {
		return
	}

	res, err = db.Exec(query)

	return
}

func (u *updateStatementImpl) SET(values ...interface{}) UpdateStatement {

	for _, value := range values {
		if clause, ok := value.(Clause); ok {
			u.updateValues = append(u.updateValues, clause)
		} else {
			u.updateValues = append(u.updateValues, Literal(value))
		}
	}

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

func (u *updateStatementImpl) String() (sql string, err error) {
	buf := new(bytes.Buffer)
	_, _ = buf.WriteString("UPDATE ")

	if u.table == nil {
		return "", errors.Newf("nil tableName.  Generated sql: %s", buf.String())
	}

	if err = u.table.SerializeSql(buf); err != nil {
		return
	}

	if len(u.updateValues) == 0 {
		return "", errors.Newf(
			"No column updated.  Generated sql: %s",
			buf.String())
	}

	_, _ = buf.WriteString(" SET")

	if len(u.columns) > 1 {
		buf.WriteString(" ( ")
	} else {
		buf.WriteString(" ")
	}

	for i, column := range u.columns {
		if i > 0 {
			buf.WriteString(", ")
		}

		buf.WriteString(column.Name())

		if err != nil {
			return
		}
	}

	if len(u.columns) > 1 {
		buf.WriteString(" )")
	}

	buf.WriteString(" =")

	if len(u.updateValues) > 1 {
		buf.WriteString(" (")
	}

	for i, value := range u.updateValues {
		if i > 0 {
			buf.WriteString(", ")
		}

		err = value.SerializeSql(buf)

		if err != nil {
			return
		}
	}

	if len(u.updateValues) > 1 {
		buf.WriteString(" )")
	}

	if u.where == nil {
		return "", errors.Newf(
			"Updating without a WHERE clause.  Generated sql: %s",
			buf.String())
	}

	_, _ = buf.WriteString(" WHERE ")
	if err = u.where.SerializeSql(buf); err != nil {
		return
	}

	if len(u.returning) > 0 {
		buf.WriteString(" RETURNING ")

		err = serializeProjectionList(u.returning, buf)

		if err != nil {
			return
		}
	}

	return buf.String() + ";", nil
}
