package jet

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-jet/jet/execution"
)

type InsertStatement interface {
	Statement

	// Insert row of values
	VALUES(value interface{}, values ...interface{}) InsertStatement
	// Insert row of values, where value for each column is extracted from filed of structure data.
	// If data is not struct or there is no field for every column selected, this method will panic.
	MODEL(data interface{}) InsertStatement

	MODELS(data interface{}) InsertStatement

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

func (i *insertStatementImpl) MODEL(data interface{}) InsertStatement {
	i.rows = append(i.rows, unwindRowFromModel(i.getColumns(), data))
	return i
}

func (i *insertStatementImpl) MODELS(data interface{}) InsertStatement {
	i.rows = append(i.rows, unwindRowsFromModels(i.getColumns(), data)...)
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

func (i *insertStatementImpl) getColumns() []column {
	if len(i.columns) > 0 {
		return i.columns
	}

	return i.table.columns()
}

func (i *insertStatementImpl) DebugSql() (query string, err error) {
	return debugSql(i)
}

func (i *insertStatementImpl) Sql() (sql string, args []interface{}, err error) {
	queryData := &sqlBuilder{}

	queryData.newLine()
	queryData.writeString("INSERT INTO")

	if isNil(i.table) {
		return "", nil, errors.New("jet: table is nil")
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
		return "", nil, errors.New("jet: no row values or query specified")
	}

	if len(i.rows) > 0 && i.query != nil {
		return "", nil, errors.New("jet: only row values or query has to be specified")
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

	if err = queryData.writeReturning(insert_statement, i.returning); err != nil {
		return
	}

	sql, args = queryData.finalize()

	return
}

func (i *insertStatementImpl) Query(db execution.DB, destination interface{}) error {
	return query(i, db, destination)
}

func (i *insertStatementImpl) QueryContext(db execution.DB, context context.Context, destination interface{}) error {
	return queryContext(i, db, context, destination)
}

func (i *insertStatementImpl) Exec(db execution.DB) (res sql.Result, err error) {
	return exec(i, db)
}

func (i *insertStatementImpl) ExecContext(db execution.DB, context context.Context) (res sql.Result, err error) {
	return execContext(i, db, context)
}
