package jet

import (
	"errors"
	"github.com/serenize/snaker"
	"reflect"
	"strings"
)

func serializeOrderByClauseList(statement statementType, orderByClauses []OrderByClause, out *queryData) error {

	for i, value := range orderByClauses {
		if i > 0 {
			out.writeString(", ")
		}

		err := value.serializeForOrderBy(statement, out)

		if err != nil {
			return err
		}
	}

	return nil
}

func serializeGroupByClauseList(statement statementType, clauses []groupByClause, out *queryData) (err error) {

	for i, c := range clauses {
		if i > 0 {
			out.writeString(", ")
		}

		if c == nil {
			return errors.New("nil clause.")
		}

		if err = c.serializeForGroupBy(statement, out); err != nil {
			return
		}
	}

	return nil
}

func serializeClauseList(statement statementType, clauses []clause, out *queryData) (err error) {

	for i, c := range clauses {
		if i > 0 {
			out.writeString(", ")
		}

		if c == nil {
			return errors.New("nil clause.")
		}

		if err = c.serialize(statement, out); err != nil {
			return
		}
	}

	return nil
}

func serializeExpressionList(statement statementType, expressions []Expression, separator string, out *queryData) error {

	for i, value := range expressions {
		if i > 0 {
			out.writeString(separator)
		}

		err := value.serialize(statement, out)

		if err != nil {
			return err
		}
	}

	return nil
}

func serializeProjectionList(statement statementType, projections []projection, out *queryData) error {
	for i, col := range projections {
		if i > 0 {
			out.writeString(",")
			out.newLine()
		}

		if col == nil {
			return errors.New("projection is nil")
		}

		if err := col.serializeForProjection(statement, out); err != nil {
			return err
		}
	}

	return nil
}

func serializeColumnNames(columns []column, out *queryData) error {
	for i, col := range columns {
		if i > 0 {
			out.writeString(", ")
		}

		if col == nil {
			return errors.New("nil column in columns list")
		}

		out.writeString(col.Name())
	}

	return nil
}

func columnListToProjectionList(columns []Column) []projection {
	var ret []projection

	for _, column := range columns {
		ret = append(ret, column)
	}

	return ret
}

func isNil(v interface{}) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}

func valueToClause(value interface{}) clause {
	if clause, ok := value.(clause); ok {
		return clause
	} else {
		return literal(value)
	}
}

func unwindRowFromModel(columns []column, data interface{}) []clause {
	structValue := reflect.Indirect(reflect.ValueOf(data))

	row := []clause{}

	mustBe(structValue, reflect.Struct)

	for _, column := range columns {
		columnName := column.Name()
		structFieldName := snaker.SnakeToCamel(columnName)

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

func unwindRowsFromModels(columns []column, data interface{}) [][]clause {
	sliceValue := reflect.Indirect(reflect.ValueOf(data))
	mustBe(sliceValue, reflect.Slice)

	rows := [][]clause{}

	for i := 0; i < sliceValue.Len(); i++ {
		structValue := sliceValue.Index(i)

		rows = append(rows, unwindRowFromModel(columns, structValue.Interface()))
	}

	return rows
}

func unwindRowFromValues(value interface{}, values []interface{}) []clause {
	row := []clause{}

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
