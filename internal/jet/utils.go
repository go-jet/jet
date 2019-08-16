package jet

import (
	"github.com/go-jet/jet/internal/utils"
	"reflect"
)

func serializeOrderByClauseList(statement StatementType, orderByClauses []OrderByClause, out *SqlBuilder) {

	for i, value := range orderByClauses {
		if i > 0 {
			out.WriteString(", ")
		}

		value.serializeForOrderBy(statement, out)
	}
}

func serializeGroupByClauseList(statement StatementType, clauses []GroupByClause, out *SqlBuilder) {

	for i, c := range clauses {
		if i > 0 {
			out.WriteString(", ")
		}

		if c == nil {
			panic("jet: nil clause")
		}

		c.serializeForGroupBy(statement, out)
	}
}

func SerializeClauseList(statement StatementType, clauses []Serializer, out *SqlBuilder) {

	for i, c := range clauses {
		if i > 0 {
			out.WriteString(", ")
		}

		if c == nil {
			panic("jet: nil clause")
		}

		c.serialize(statement, out)
	}
}

func serializeExpressionList(statement StatementType, expressions []Expression, separator string, out *SqlBuilder) {

	for i, value := range expressions {
		if i > 0 {
			out.WriteString(separator)
		}

		value.serialize(statement, out)
	}
}

func SerializeProjectionList(statement StatementType, projections []Projection, out *SqlBuilder) {
	for i, col := range projections {
		if i > 0 {
			out.WriteString(",")
			out.NewLine()
		}

		if col == nil {
			panic("jet: Projection is nil")
		}

		col.serializeForProjection(statement, out)
	}
}

func SerializeColumnNames(columns []Column, out *SqlBuilder) {
	for i, col := range columns {
		if i > 0 {
			out.WriteString(", ")
		}

		if col == nil {
			panic("jet: nil column in columns list")
		}

		out.WriteString(col.Name())
	}
}

func ColumnListToProjectionList(columns []ColumnExpression) []Projection {
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

func UnwindRowFromModel(columns []Column, data interface{}) []Serializer {
	structValue := reflect.Indirect(reflect.ValueOf(data))

	row := []Serializer{}

	utils.ValueMustBe(structValue, reflect.Struct, "jet: data has to be a struct")

	for _, column := range columns {
		columnName := column.Name()
		structFieldName := utils.ToGoIdentifier(columnName)

		structField := structValue.FieldByName(structFieldName)

		if !structField.IsValid() {
			panic("missing struct field for column : " + columnName)
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

func UnwindRowsFromModels(columns []Column, data interface{}) [][]Serializer {
	sliceValue := reflect.Indirect(reflect.ValueOf(data))
	utils.ValueMustBe(sliceValue, reflect.Slice, "jet: data has to be a slice.")

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
