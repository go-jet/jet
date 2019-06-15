package sqlbuilder

import (
	"database/sql"
	"errors"
	"github.com/go-jet/jet/sqlbuilder/execution"
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
}

func (d *deleteStatementImpl) WHERE(expression BoolExpression) DeleteStatement {
	d.where = expression
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
	return DebugSql(d)
}

func (d *deleteStatementImpl) Query(db execution.Db, destination interface{}) error {
	return Query(d, db, destination)
}

func (d *deleteStatementImpl) Exec(db execution.Db) (res sql.Result, err error) {
	return Exec(d, db)
}
