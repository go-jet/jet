package sqlbuilder

import (
	"database/sql"
	"errors"
	"github.com/go-jet/jet/sqlbuilder/execution"
)

type InsertStatement interface {
	Statement

	// Add a row of values to the insert Statement.
	VALUES(value interface{}, values ...interface{}) InsertStatement
	// Model structure mapped to column names
	USING(data interface{}) InsertStatement

	QUERY(selectStatement SelectStatement) InsertStatement

	RETURNING(projections ...projection) InsertStatement
}

func newInsertStatement(t WritableTable, columns []column) InsertStatement {
	return &insertStatementImpl{
		table:   t,
		columns: columns,
	}
}

type insertStatementImpl struct {
	table     WritableTable
	columns   []column
	rows      [][]clause
	query     SelectStatement
	returning []projection
}

func (i *insertStatementImpl) VALUES(value interface{}, values ...interface{}) InsertStatement {
	i.rows = append(i.rows, unwindRowFromValues(value, values))
	return i
}

func (i *insertStatementImpl) USING(data interface{}) InsertStatement {
	i.rows = append(i.rows, unwindRowFromModel(i.columns, data))
	return i
}

func (i *insertStatementImpl) RETURNING(projections ...projection) InsertStatement {
	i.returning = projections
	return i
}

func (i *insertStatementImpl) QUERY(selectStatement SelectStatement) InsertStatement {
	i.query = selectStatement
	return i
}

func (i *insertStatementImpl) DebugSql() (query string, err error) {
	return DebugSql(i)
}

func (i *insertStatementImpl) Sql() (sql string, args []interface{}, err error) {
	queryData := &queryData{}

	queryData.newLine()
	queryData.writeString("INSERT INTO")

	if isNil(i.table) {
		return "", nil, errors.New("table is nil")
	}

	err = i.table.serialize(insert_statement, queryData)

	if err != nil {
		return
	}

	if len(i.columns) > 0 {
		queryData.writeString("(")

		err = serializeColumnNames(i.columns, queryData)

		if err != nil {
			return
		}

		queryData.writeString(")")
	}

	if len(i.rows) == 0 && i.query == nil {
		return "", nil, errors.New("no row values or query  specified")
	}

	if len(i.rows) > 0 && i.query != nil {
		return "", nil, errors.New("only row values or query has to be specified")
	}

	if len(i.rows) > 0 {
		queryData.writeString("VALUES")

		for row_i, row := range i.rows {
			if row_i > 0 {
				queryData.writeString(",")
			}

			queryData.increaseIdent()
			queryData.newLine()
			queryData.writeString("(")

			if len(row) != len(i.columns) {
				return "", nil, errors.New("number of values does not match number of columns")
			}

			err = serializeClauseList(insert_statement, row, queryData)

			if err != nil {
				return "", nil, err
			}

			queryData.writeByte(')')
			queryData.decreaseIdent()
		}
	}

	if i.query != nil {
		err = i.query.serialize(insert_statement, queryData)

		if err != nil {
			return
		}
	}

	if len(i.returning) > 0 {
		queryData.newLine()
		queryData.writeString("RETURNING")

		err = queryData.writeProjections(insert_statement, i.returning)

		if err != nil {
			return
		}
	}

	sql, args = queryData.finalize()

	return
}

func (i *insertStatementImpl) Query(db execution.Db, destination interface{}) error {
	return Query(i, db, destination)
}

func (i *insertStatementImpl) Exec(db execution.Db) (res sql.Result, err error) {
	return Exec(i, db)
}
