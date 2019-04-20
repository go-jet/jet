package sqlbuilder

import (
	"bytes"
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/serenize/snaker"
	"github.com/sub0zero/go-sqlbuilder/types"
	"reflect"
	"strings"
)

type InsertStatement interface {
	Statement

	// Add a row of values to the insert statement.
	VALUES(values ...interface{}) InsertStatement
	// Map or stracture mapped to column names
	VALUES_MAPPING(data interface{}) InsertStatement

	RETURNING(projections ...Projection) InsertStatement

	QUERY(selectStatement SelectStatement) InsertStatement
}

func newInsertStatement(t WritableTable, columns ...Column) InsertStatement {
	return &insertStatementImpl{
		table:   t,
		columns: columns,
	}
}

type columnAssignment struct {
	col  Column
	expr Expression
}

type insertStatementImpl struct {
	table     WritableTable
	columns   []Column
	rows      [][]Clause
	query     SelectStatement
	returning []Projection

	errors []string
}

func (s *insertStatementImpl) Query(db types.Db, destination interface{}) error {
	return Query(s, db, destination)
}

func (u *insertStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
	return Execute(u, db)
}

func (s *insertStatementImpl) VALUES(values ...interface{}) InsertStatement {
	literalRow := []Clause{}

	for _, value := range values {
		if clause, ok := value.(Clause); ok {
			literalRow = append(literalRow, clause)
		} else {
			literalRow = append(literalRow, Literal(value))
		}
	}

	s.rows = append(s.rows, literalRow)
	return s
}

func (i *insertStatementImpl) VALUES_MAPPING(data interface{}) InsertStatement {
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

	rowValues := []Clause{}

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

func (i *insertStatementImpl) RETURNING(projections ...Projection) InsertStatement {
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

func (s *insertStatementImpl) String() (sql string, err error) {
	buf := new(bytes.Buffer)
	_, _ = buf.WriteString("INSERT ")
	_, _ = buf.WriteString("INTO ")

	if len(s.errors) > 0 {
		return "", errors.New("sql builder errors: " + strings.Join(s.errors, ", "))
	}

	if s.table == nil {
		return "", errors.Newf("nil tableName.  Generated sql: %s", buf.String())
	}

	buf.WriteString(s.table.SchemaName() + "." + s.table.TableName())

	if len(s.columns) > 0 {
		_, _ = buf.WriteString(" (")
		for i, col := range s.columns {
			if i > 0 {
				_ = buf.WriteByte(',')
			}

			if col == nil {
				return "", errors.Newf(
					"nil column in columns list.  Generated sql: %s",
					buf.String())
			}

			buf.WriteString(col.Name())
		}

		buf.WriteString(") ")
	}

	if len(s.rows) == 0 && s.query == nil {
		return "", errors.Newf("No row or query  specified.  Generated sql: %s", buf.String())
	}

	if len(s.rows) > 0 && s.query != nil {
		return "", errors.Newf("Only new rows or query has to be specified.  Generated sql: %s", buf.String())
	}

	if len(s.rows) > 0 {
		_, _ = buf.WriteString("VALUES (")
		for row_i, row := range s.rows {
			if row_i > 0 {
				_, _ = buf.WriteString(", (")
			}

			if len(row) != len(s.columns) {
				return "", errors.Newf(
					"# of values does not match # of columns.  Generated sql: %s",
					buf.String())
			}

			for col_i, value := range row {
				if col_i > 0 {
					_ = buf.WriteByte(',')
				}

				if value == nil {
					return "", errors.Newf(
						"nil value in row %d col %d.  Generated sql: %s",
						row_i,
						col_i,
						buf.String())
				}

				if err = value.SerializeSql(buf); err != nil {
					return
				}
			}
			_ = buf.WriteByte(')')
		}
	}

	if s.query != nil {
		err = s.query.SerializeSql(buf)

		if err != nil {
			return
		}
	}

	if len(s.returning) > 0 {
		buf.WriteString(" RETURNING ")

		err = serializeProjectionList(s.returning, buf)

		if err != nil {
			return
		}
	}

	buf.WriteByte(';')

	return buf.String(), nil
}
