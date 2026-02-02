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

// SerializeProjectionListJsonObj serializes a list of projections for JSON object
func SerializeProjectionListJsonObj(statement StatementType, projections []Projection, out *SQLBuilder) {

	for i, p := range projections {
		if i > 0 {
			out.WriteString(",")
			out.NewLine()
		}

		if p == nil {
			panic("jet: Projection is nil")
		}

		p.serializeForJsonObjEntry(statement, out)
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

// ToSerializerList converts list of expressions to list of serializers
func ToSerializerList[T Serializer](elems []T) []Serializer {
	ret := make([]Serializer, len(elems))

	for i, ser := range elems {
		ret[i] = ser
	}

	return ret
}

// ToExpressionList converts list of any expressions to list of expressions
func ToExpressionList[T Expression](expressions []T) []Expression {
	ret := make([]Expression, len(expressions))

	for i, expr := range expressions {
		ret[i] = expr
	}

	return ret
}

// ColumnListToProjectionList func
func ColumnListToProjectionList(columns []ColumnExpression) []Projection {
	ret := make([]Projection, len(columns))

	for i, column := range columns {
		ret[i] = column
	}

	return ret
}

// ToSerializerValue creates Serializer type from the value
func ToSerializerValue(value interface{}) Serializer {
	if clause, ok := value.(Serializer); ok {
		return clause
	}

	return Literal(value)
}

// UnwindRowFromModel func
func UnwindRowFromModel(columns []Column, data interface{}) []Serializer {
	structValue := reflect.Indirect(reflect.ValueOf(data))

	row := make([]Serializer, len(columns))

	must.ValueBeOfTypeKind(structValue, reflect.Struct, "jet: data has to be a struct")

	for i, column := range columns {
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

		row[i] = Literal(field)
	}

	return row
}

// UnwindRowsFromModels func
func UnwindRowsFromModels(columns []Column, data interface{}) [][]Serializer {
	sliceValue := reflect.Indirect(reflect.ValueOf(data))
	must.ValueBeOfTypeKind(sliceValue, reflect.Slice, "jet: data has to be a slice.")

	sliceLen := sliceValue.Len()
	rows := make([][]Serializer, sliceLen)

	for i := 0; i < sliceLen; i++ {
		structValue := sliceValue.Index(i)

		rows[i] = UnwindRowFromModel(columns, structValue.Interface())
	}

	return rows
}

// UnwindRowFromValues func
func UnwindRowFromValues(value interface{}, values []interface{}) []Serializer {
	allValues := append([]interface{}{value}, values...)

	row := make([]Serializer, len(allValues))

	for i, val := range allValues {
		row[i] = ToSerializerValue(val)
	}

	return row
}

// UnwidColumnList func
func UnwidColumnList(columns []Column) []Column {
	var ret []Column

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

// OptionalOrDefault will return first value from variable argument list expression or
// defaultExpression if variable argument list is empty
func OptionalOrDefault(expressions []Expression, defaultExpression Expression) Expression {
	if len(expressions) > 0 {
		return expressions[0]
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

func singleOptional[T any](value []T) T {
	if len(value) > 0 {
		return value[0]
	}

	var def T

	return def
}
