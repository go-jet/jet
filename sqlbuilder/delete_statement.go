package sqlbuilder

import (
	"bytes"
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/types"
)

type DeleteStatement interface {
	Statement

	WHERE(expression BoolExpression) DeleteStatement
}

func newDeleteStatement(table WritableTable) DeleteStatement {
	return &deleteStatementImpl{
		table: table,
	}
}

type deleteStatementImpl struct {
	table WritableTable
	where BoolExpression
	order *listClause
}

func (u *deleteStatementImpl) Query(db types.Db, destination interface{}) error {
	return Query(u, db, destination)
}

func (u *deleteStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
	return Execute(u, db)
}

func (d *deleteStatementImpl) WHERE(expression BoolExpression) DeleteStatement {
	d.where = expression
	return d
}

func (d *deleteStatementImpl) String() (sql string, err error) {
	buf := new(bytes.Buffer)
	_, _ = buf.WriteString("DELETE FROM ")

	if d.table == nil {
		return "", errors.Newf("nil tableName.  Generated sql: %s", buf.String())
	}

	if err = d.table.SerializeSql(buf); err != nil {
		return
	}

	if d.where == nil {
		return "", errors.Newf("Deleting without a WHERE clause.  Generated sql: %s", buf.String())
	}

	_, _ = buf.WriteString(" WHERE ")
	if err = d.where.SerializeSql(buf); err != nil {
		return
	}

	if d.order != nil {
		_, _ = buf.WriteString(" ORDER BY ")
		if err = d.order.SerializeSql(buf); err != nil {
			return
		}
	}

	return buf.String() + ";", nil
}
