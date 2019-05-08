package sqlbuilder

import (
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/types"
)

type deleteStatement interface {
	statement

	WHERE(expression boolExpression) deleteStatement
}

func newDeleteStatement(table writableTable) deleteStatement {
	return &deleteStatementImpl{
		table: table,
	}
}

type deleteStatementImpl struct {
	table writableTable
	where boolExpression
}

func (d *deleteStatementImpl) WHERE(expression boolExpression) deleteStatement {
	d.where = expression
	return d
}

func (d *deleteStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := &queryData{}

	queryData.writeString("DELETE FROM ")

	if d.table == nil {
		return "", nil, errors.New("nil tableName.")
	}

	if err = d.table.serialize(delete_statement, queryData); err != nil {
		return
	}

	if d.where == nil {
		return "", nil, errors.New("Deleting without a WHERE clause.")
	}

	if err = queryData.writeWhere(delete_statement, d.where); err != nil {
		return
	}

	return queryData.buff.String() + ";", queryData.args, nil
}

func (u *deleteStatementImpl) Query(db types.Db, destination interface{}) error {
	return Query(u, db, destination)
}

func (u *deleteStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
	return Execute(u, db)
}
