package sqlbuilder

import (
	"database/sql"
	"errors"
	"github.com/go-jet/jet/sqlbuilder/execution"
	"github.com/serenize/snaker"
	"reflect"
	"strings"
)

type InsertStatement interface {
	Statement

	// Add a row of values to the insert Statement.
	VALUES(values ...interface{}) InsertStatement
	// Model structure mapped to column names
	MODEL(data interface{}) InsertStatement

	QUERY(selectStatement SelectStatement) InsertStatement

	RETURNING(projections ...projection) InsertStatement
}

func newInsertStatement(t WritableTable, columns ...Column) InsertStatement {
	return &insertStatementImpl{
		table:   t,
		columns: columns,
	}
}

type insertStatementImpl struct {
	table     WritableTable
	columns   []Column
	rows      [][]clause
	query     SelectStatement
	returning []projection

	errors []string
}

func (i *insertStatementImpl) Query(db execution.Db, destination interface{}) error {
	return Query(i, db, destination)
}

func (i *insertStatementImpl) Execute(db execution.Db) (res sql.Result, err error) {
	return Execute(i, db)
}

func (i *insertStatementImpl) VALUES(values ...interface{}) InsertStatement {
	if len(values) == 0 {
		return i
	}

	literalRow := []clause{}

	for _, value := range values {
		if clause, ok := value.(clause); ok {
			literalRow = append(literalRow, clause)
		} else {
			literalRow = append(literalRow, literal(value))
		}
	}

	i.rows = append(i.rows, literalRow)
	return i
}

func (i *insertStatementImpl) MODEL(data interface{}) InsertStatement {
	if data == nil {
		i.addError("MODEL : data is nil.")
		return i
	}

	value := reflect.Indirect(reflect.ValueOf(data))

	if value.Kind() != reflect.Struct {
		i.addError("MODEL : data is not struct or pointer to struct.")
		return i
	}

	rowValues := []clause{}

	for _, column := range i.columns {
		columnName := column.Name()
		structFieldName := snaker.SnakeToCamel(columnName)

		structField := value.FieldByName(structFieldName)

		if !structField.IsValid() {
			i.addError("MODEL : Data structure doesn't contain field for column " + columnName)
			return i
		}

		var field interface{}

		fieldValue := reflect.Indirect(structField)

		if fieldValue.IsValid() {
			field = fieldValue.Interface()
		} else {
			field = nil
		}

		rowValues = append(rowValues, literal(field))
	}

	i.rows = append(i.rows, rowValues)

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

func (i *insertStatementImpl) addError(err string) {
	i.errors = append(i.errors, err)
}

func (i *insertStatementImpl) DebugSql() (query string, err error) {
	return DebugSql(i)
}

func (i *insertStatementImpl) Sql() (sql string, args []interface{}, err error) {
	if len(i.errors) > 0 {
		return "", nil, errors.New("errors: " + strings.Join(i.errors, ", "))
	}

	queryData := &queryData{}

	queryData.nextLine()
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

		err = serializeColumnList(insert_statement, i.columns, queryData)

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
			queryData.nextLine()
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
		queryData.nextLine()
		queryData.writeString("RETURNING")

		err = queryData.writeProjections(insert_statement, i.returning)

		if err != nil {
			return
		}
	}

	sql, args = queryData.finalize()

	return
}
