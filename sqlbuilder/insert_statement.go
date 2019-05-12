package sqlbuilder

import (
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/serenize/snaker"
	"github.com/sub0zero/go-sqlbuilder/types"
	"reflect"
	"strings"
)

type insertStatement interface {
	Statement

	// Add a row of values to the insert Statement.
	VALUES(values ...interface{}) insertStatement
	// Map or stracture mapped to column names
	VALUES_MAPPING(data interface{}) insertStatement

	RETURNING(projections ...projection) insertStatement

	QUERY(selectStatement selectStatement) insertStatement
}

func newInsertStatement(t writableTable, columns ...column) insertStatement {
	return &insertStatementImpl{
		table:   t,
		columns: columns,
	}
}

type insertStatementImpl struct {
	table     writableTable
	columns   []column
	rows      [][]clause
	query     selectStatement
	returning []projection

	errors []string
}

func (s *insertStatementImpl) Query(db types.Db, destination interface{}) error {
	return Query(s, db, destination)
}

func (u *insertStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
	return Execute(u, db)
}

// expression or default keyword
func (i *insertStatementImpl) VALUES(values ...interface{}) insertStatement {
	if len(values) == 0 {
		return i
	}

	literalRow := []clause{}

	for _, value := range values {
		if clause, ok := value.(clause); ok {
			literalRow = append(literalRow, clause)
		} else {
			literalRow = append(literalRow, Literal(value))
		}
	}

	i.rows = append(i.rows, literalRow)
	return i
}

func (i *insertStatementImpl) VALUES_MAPPING(data interface{}) insertStatement {
	if data == nil {
		i.addError("Add method data is nil.")
		return i
	}

	value := reflect.ValueOf(data)

	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	if value.Kind() != reflect.Struct {
		i.addError("Add method data is not struct or pointer to struct.")
		return i
	}

	rowValues := []clause{}

	for _, column := range i.columns {
		columnName := column.Name()
		structFieldName := snaker.SnakeToCamel(columnName)

		structField := value.FieldByName(structFieldName)

		if !structField.IsValid() {
			i.addError("Add() : Data structure doesn't contain field : " + structFieldName + " for column " + columnName)
			return i
		}

		rowValues = append(rowValues, Literal(structField.Interface()))
	}

	i.rows = append(i.rows, rowValues)

	return i
}

func (i *insertStatementImpl) RETURNING(projections ...projection) insertStatement {
	i.returning = defaultProjectionAliasing(projections)

	return i
}

func (i *insertStatementImpl) QUERY(selectStatement selectStatement) insertStatement {
	i.query = selectStatement

	return i
}

func (i *insertStatementImpl) addError(err string) {
	i.errors = append(i.errors, err)
}

func (i *insertStatementImpl) DebugSql() (query string, err error) {
	return DebugSql(i)
}

func (s *insertStatementImpl) Sql() (sql string, args []interface{}, err error) {
	if len(s.errors) > 0 {
		return "", nil, errors.New("sql builder errors: " + strings.Join(s.errors, ", "))
	}

	queryData := &queryData{}

	queryData.nextLine()
	queryData.writeString("INSERT INTO")

	if s.table == nil {
		return "", nil, errors.Newf("nil tableName.")
	}

	err = s.table.serialize(insert_statement, queryData)

	queryData.writeByte(' ')

	if err != nil {
		return "", nil, err
	}

	if len(s.columns) > 0 {
		queryData.writeString("(")

		err = serializeColumnList(insert_statement, s.columns, queryData)

		if err != nil {
			return "", nil, err
		}

		queryData.writeString(")")
	}

	if len(s.rows) == 0 && s.query == nil {
		return "", nil, errors.New("No row or query  specified.")
	}

	if len(s.rows) > 0 && s.query != nil {
		return "", nil, errors.New("Only new rows or query has to be specified.")
	}

	if len(s.rows) > 0 {
		queryData.writeString("VALUES")

		for row_i, row := range s.rows {
			if row_i > 0 {
				queryData.writeString(",")
			}

			queryData.increaseIdent()
			queryData.nextLine()
			queryData.writeString("(")

			if len(row) != len(s.columns) {
				return "", nil, errors.New("# of values does not match # of columns.")
			}

			err = serializeClauseList(insert_statement, row, queryData)

			if err != nil {
				return "", nil, err
			}

			queryData.writeByte(')')
			queryData.decreaseIdent()
		}
	}

	if s.query != nil {
		err = s.query.serialize(insert_statement, queryData)

		if err != nil {
			return
		}
	}

	if len(s.returning) > 0 {
		queryData.nextLine()
		queryData.writeString("RETURNING")

		err = queryData.writeProjection(insert_statement, s.returning)

		if err != nil {
			return
		}
	}

	sql, args = queryData.finalize()

	return
}
