package jet

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-jet/jet/execution"
	"github.com/go-jet/jet/internal/utils"
)

// InsertStatement is interface for SQL INSERT statements
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

func (i *insertStatementImpl) accept(visitor visitor) {
	visitor.visit(i)

	i.table.accept(visitor)
}

func (i *insertStatementImpl) DebugSql(dialect ...Dialect) (query string, err error) {
	return debugSql(i, dialect...)
}

func (i *insertStatementImpl) Sql(dialect ...Dialect) (query string, args []interface{}, err error) {
	out := &sqlBuilder{
		dialect: detectDialect(i, dialect...),
	}

	out.newLine()
	out.writeString("INSERT INTO")

	if utils.IsNil(i.table) {
		return "", nil, errors.New("jet: table is nil")
	}

	err = i.table.serialize(insertStatement, out)

	if err != nil {
		return
	}

	if len(i.columns) > 0 {
		out.writeString("(")

		err = serializeColumnNames(i.columns, out)

		if err != nil {
			return
		}

		out.writeString(")")
	}

	if len(i.rows) == 0 && i.query == nil {
		return "", nil, errors.New("jet: no row values or query specified")
	}

	if len(i.rows) > 0 && i.query != nil {
		return "", nil, errors.New("jet: only row values or query has to be specified")
	}

	if len(i.rows) > 0 {
		out.writeString("VALUES")

		for rowIndex, row := range i.rows {
			if rowIndex > 0 {
				out.writeString(",")
			}

			out.increaseIdent()
			out.newLine()
			out.writeString("(")

			err = serializeClauseList(insertStatement, row, out)

			if err != nil {
				return "", nil, err
			}

			out.writeByte(')')
			out.decreaseIdent()
		}
	}

	if i.query != nil {
		err = i.query.serialize(insertStatement, out)

		if err != nil {
			return
		}
	}

	if err = out.writeReturning(insertStatement, i.returning); err != nil {
		return
	}

	query, args = out.finalize()

	return
}

func (i *insertStatementImpl) Query(db execution.DB, destination interface{}) error {
	return query(i, db, destination)
}

func (i *insertStatementImpl) QueryContext(context context.Context, db execution.DB, destination interface{}) error {
	return queryContext(context, i, db, destination)
}

func (i *insertStatementImpl) Exec(db execution.DB) (res sql.Result, err error) {
	return exec(i, db)
}

func (i *insertStatementImpl) ExecContext(context context.Context, db execution.DB) (res sql.Result, err error) {
	return execContext(context, i, db)
}
