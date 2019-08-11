package jet

import (
	"errors"
	"github.com/go-jet/jet/internal/utils"
	"reflect"
	"strings"
)

func serializeOrderByClauseList(statement StatementType, orderByClauses []OrderByClause, out *SqlBuilder) error {

	for i, value := range orderByClauses {
		if i > 0 {
			out.WriteString(", ")
		}

		err := value.serializeForOrderBy(statement, out)

		if err != nil {
			return err
		}
	}

	return nil
}

func serializeGroupByClauseList(statement StatementType, clauses []GroupByClause, out *SqlBuilder) (err error) {

	for i, c := range clauses {
		if i > 0 {
			out.WriteString(", ")
		}

		if c == nil {
			return errors.New("jet: nil clause")
		}

		if err = c.serializeForGroupBy(statement, out); err != nil {
			return
		}
	}

	return nil
}

func SerializeClauseList(statement StatementType, clauses []Serializer, out *SqlBuilder) (err error) {

	for i, c := range clauses {
		if i > 0 {
			out.WriteString(", ")
		}

		if c == nil {
			return errors.New("jet: nil clause")
		}

		if err = c.serialize(statement, out); err != nil {
			return
		}
	}

	return nil
}

func serializeExpressionList(statement StatementType, expressions []Expression, separator string, out *SqlBuilder) error {

	for i, value := range expressions {
		if i > 0 {
			out.WriteString(separator)
		}

		err := value.serialize(statement, out)

		if err != nil {
			return err
		}
	}

	return nil
}

func SerializeProjectionList(statement StatementType, projections []Projection, out *SqlBuilder) error {
	for i, col := range projections {
		if i > 0 {
			out.WriteString(",")
			out.newLine()
		}

		if col == nil {
			return errors.New("jet: Projection is nil")
		}

		if err := col.serializeForProjection(statement, out); err != nil {
			return err
		}
	}

	return nil
}

func SerializeColumnNames(columns []IColumn, out *SqlBuilder) error {
	for i, col := range columns {
		if i > 0 {
			out.WriteString(", ")
		}

		if col == nil {
			return errors.New("jet: nil column in columns list")
		}

		out.WriteString(col.Name())
	}

	return nil
}

func ColumnListToProjectionList(columns []Column) []Projection {
	var ret []Projection

	for _, column := range columns {
		ret = append(ret, column)
	}

	return ret
}

func valueToClause(value interface{}) Serializer {
	if clause, ok := value.(Serializer); ok {
		return clause
	}

	return literal(value)
}

func UnwindRowFromModel(columns []IColumn, data interface{}) []Serializer {
	structValue := reflect.Indirect(reflect.ValueOf(data))

	row := []Serializer{}

	mustBe(structValue, reflect.Struct)

	for _, column := range columns {
		columnName := column.Name()
		structFieldName := utils.ToGoIdentifier(columnName)

		structField := structValue.FieldByName(structFieldName)

		if !structField.IsValid() {
			panic("missing struct field for column : " + column.Name())
		}

		var field interface{}

		if structField.Kind() == reflect.Ptr && structField.IsNil() {
			field = nil
		} else {
			field = reflect.Indirect(structField).Interface()
		}

		row = append(row, literal(field))
	}

	return row
}

func UnwindRowsFromModels(columns []IColumn, data interface{}) [][]Serializer {
	sliceValue := reflect.Indirect(reflect.ValueOf(data))
	mustBe(sliceValue, reflect.Slice)

	rows := [][]Serializer{}

	for i := 0; i < sliceValue.Len(); i++ {
		structValue := sliceValue.Index(i)

		rows = append(rows, UnwindRowFromModel(columns, structValue.Interface()))
	}

	return rows
}

func UnwindRowFromValues(value interface{}, values []interface{}) []Serializer {
	row := []Serializer{}

	allValues := append([]interface{}{value}, values...)

	for _, val := range allValues {
		row = append(row, valueToClause(val))
	}

	return row
}

func mustBe(v reflect.Value, expectedKinds ...reflect.Kind) {
	indirectV := reflect.Indirect(v)
	types := []string{}

	for _, expectedKind := range expectedKinds {
		types = append(types, expectedKind.String())
		if k := indirectV.Kind(); k == expectedKind {
			return
		}
	}

	panic("argument mismatch: expected " + strings.Join(types, " or ") + ", got " + v.Type().String())
}
