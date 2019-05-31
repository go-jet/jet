package sqlbuilder

import (
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/sqlbuilder/execution"
)

type deleteStatement interface {
	Statement

	WHERE(expression BoolExpression) deleteStatement
}

func newDeleteStatement(table writableTable) deleteStatement {
	return &deleteStatementImpl{
		table: table,
	}
}

type deleteStatementImpl struct {
	table writableTable
	where BoolExpression
}

func (d *deleteStatementImpl) WHERE(expression BoolExpression) deleteStatement {
	d.where = expression
	return d
}

func (d *deleteStatementImpl) serializeImpl(out *queryData) error {
	if d == nil {
		return errors.New("Delete statement. ")
	}
	out.nextLine()
	out.writeString("DELETE FROM")

	if d.table == nil {
		return errors.New("nil tableName.")
	}

	if err := d.table.serialize(delete_statement, out); err != nil {
		return err
	}

	if d.where == nil {
		return errors.New("Deleting without a WHERE clause.")
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

func (d *deleteStatementImpl) Execute(db execution.Db) (res sql.Result, err error) {
	return Execute(d, db)
}
