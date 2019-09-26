package jet

import (
	"github.com/go-jet/jet/internal/utils"
	"reflect"
)

// SerializeClauseList func
func SerializeClauseList(statement StatementType, clauses []Serializer, out *SQLBuilder) {

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

func serializeExpressionList(statement StatementType, expressions []Expression, separator string, out *SQLBuilder) {

	for i, value := range expressions {
		if i > 0 {
			out.WriteString(separator)
		}

		value.serialize(statement, out)
	}
}

// SerializeProjectionList func
func SerializeProjectionList(statement StatementType, projections []Projection, out *SQLBuilder) {
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

// SerializeColumnNames func
func SerializeColumnNames(columns []Column, out *SQLBuilder) {
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

// ColumnListToProjectionList func
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

// UnwindRowFromModel func
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

// UnwindRowsFromModels func
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

// UnwindRowFromValues func
func UnwindRowFromValues(value interface{}, values []interface{}) []Serializer {
	row := []Serializer{}

	allValues := append([]interface{}{value}, values...)

	for _, val := range allValues {
		row = append(row, valueToClause(val))
	}

	return row
}

// UnwindColumns func
func UnwindColumns(column1 Column, columns ...Column) []Column {
	columnList := []Column{}

	if val, ok := column1.(ColumnList); ok {
		for _, col := range val {
			columnList = append(columnList, col)
		}
		columnList = append(columnList, columns...)
	} else {
		columnList = append(columnList, column1)
		columnList = append(columnList, columns...)
	}

	return columnList
}

// UnwidColumnList func
func UnwidColumnList(columns []Column) []Column {
	ret := []Column{}

	for _, col := range columns {
		if columnList, ok := col.(ColumnList); ok {
			for _, c := range columnList {
				ret = append(ret, c)
			}
		} else {
			ret = append(ret, col)
		}
	}

	return ret
}
