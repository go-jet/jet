package jet

import (
	"reflect"
	"strings"

	"github.com/go-jet/jet/v2/internal/utils/dbidentifier"
	"github.com/go-jet/jet/v2/internal/utils/must"
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

func serializeExpressionList(
	statement StatementType,
	expressions []Expression,
	separator string,
	out *SQLBuilder,
	options ...SerializeOption) {

	for i, expression := range expressions {
		if i > 0 {
			out.WriteString(separator)
		}

		if expression != nil {
			expression.serialize(statement, out, options...)
		}
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

		out.WriteIdentifier(col.Name())
	}
}

// SerializeColumnExpressions func
func SerializeColumnExpressions(columns []ColumnExpression, statementType StatementType,
	out *SQLBuilder, options ...SerializeOption) {
	for i, col := range columns {
		if i > 0 {
			out.WriteString(", ")
		}

		if col == nil {
			panic("jet: nil column in columns list")
		}

		col.serialize(statementType, out, options...)
	}
}

// SerializeColumnExpressionNames func
func SerializeColumnExpressionNames(columns []ColumnExpression, out *SQLBuilder) {
	for i, col := range columns {
		if i > 0 {
			out.WriteString(", ")
		}

		if col == nil {
			panic("jet: nil column in columns list")
		}

		out.WriteIdentifier(col.Name())
	}
}

// ExpressionListToSerializerList converts list of expressions to list of serializers
func ExpressionListToSerializerList(expressions []Expression) []Serializer {
	var ret []Serializer

	for _, expr := range expressions {
		ret = append(ret, expr)
	}

	return ret
}

// BoolExpressionListToExpressionList converts list of bool expressions to list of expressions
func BoolExpressionListToExpressionList(expressions []BoolExpression) []Expression {
	var ret []Expression

	for _, expression := range expressions {
		ret = append(ret, expression)
	}

	return ret
}

// ColumnListToProjectionList func
func ColumnListToProjectionList(columns []ColumnExpression) []Projection {
	var ret []Projection

	for _, column := range columns {
		ret = append(ret, column)
	}

	return ret
}

// ToSerializerValue creates Serializer type from the value
func ToSerializerValue(value interface{}) Serializer {
	if clause, ok := value.(Serializer); ok {
		return clause
	}

	return literal(value)
}

// UnwindRowFromModel func
func UnwindRowFromModel(columns []Column, data interface{}) []Serializer {
	structValue := reflect.Indirect(reflect.ValueOf(data))

	row := []Serializer{}

	must.ValueBeOfTypeKind(structValue, reflect.Struct, "jet: data has to be a struct")

	for _, column := range columns {
		columnName := column.Name()
		structFieldName := dbidentifier.ToGoIdentifier(columnName)

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
	must.ValueBeOfTypeKind(sliceValue, reflect.Slice, "jet: data has to be a slice.")

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
		row = append(row, ToSerializerValue(val))
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

// OptionalOrDefaultString will return first value from variable argument list str or
// defaultStr if variable argument list is empty
func OptionalOrDefaultString(defaultStr string, str ...string) string {
	if len(str) > 0 {
		return str[0]
	}

	return defaultStr
}

// OptionalOrDefaultExpression will return first value from variable argument list expression or
// defaultExpression if variable argument list is empty
func OptionalOrDefaultExpression(defaultExpression Expression, expression ...Expression) Expression {
	if len(expression) > 0 {
		return expression[0]
	}

	return defaultExpression
}

func extractTableAndColumnName(alias string) (tableName string, columnName string) {
	parts := strings.Split(alias, ".")

	if len(parts) >= 2 {
		tableName = parts[0]
		columnName = parts[1]
	} else {
		columnName = parts[0]
	}

	return
}

func serializeToDefaultDebugString(expr Serializer) string {
	out := SQLBuilder{Dialect: defaultDialect, Debug: true}
	expr.serialize(SelectStatementType, &out)
	return out.Buff.String()
}

// joinAlias examples:
//
//	joinAlias("foo", "bar") // "foo.bar"
//	joinAlias("foo.*", "bar") // "foo.bar"
//	joinAlias("", "bar") // "bar"
func joinAlias(tableAlias, columnAlias string) string {
	if tableAlias == "" {
		return columnAlias
	}
	return strings.TrimRight(tableAlias, ".*") + "." + columnAlias
}
