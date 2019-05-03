package sqlbuilder

import (
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
}

func (d *deleteStatementImpl) WHERE(expression BoolExpression) DeleteStatement {
	d.where = expression
	return d
}

func (d *deleteStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := &queryData{}
	queryData.statementType = delete_statement

	queryData.WriteString("DELETE FROM ")

	if d.table == nil {
		return "", nil, errors.New("nil tableName.")
	}

	if err = d.table.SerializeSql(queryData); err != nil {
		return
	}

	if d.where == nil {
		return "", nil, errors.New("Deleting without a WHERE clause.")
	}

	if err = queryData.WriteWhere(d.where); err != nil {
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
