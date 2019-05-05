// Query building functions for expression components
package sqlbuilder

import (
	"strconv"
	"strings"
	"time"
)

// Representation of a tuple enclosed, comma separated list of clauses
//type listClause struct {
//	clauses            []Clause
//	includeParentheses bool
//}
//
//func (list *listClause) Serialize(out *queryData, options ...serializeOption) error {
//	if list.includeParentheses {
//		out.WriteByte('(')
//	}
//
//	if err := serializeClauseList(list.clauses, out); err != nil {
//		return err
//	}
//
//	if list.includeParentheses {
//		out.WriteByte(')')
//	}
//	return nil
//}

//
//type funcExpression struct {
//	expressionInterfaceImpl
//	funcName string
//	args     *listClause
//}
//
//func (c *funcExpression) Serialize(out *queryData, options ...serializeOption) error {
//	if !validIdentifierName(c.funcName) {
//		return errors.Newf(
//			"Invalid function name: %s.",
//			c.funcName,
//			out.String())
//	}
//	_, _ = out.WriteString(c.funcName)
//	if c.args == nil {
//		_, _ = out.WriteString("()")
//	} else {
//		return c.args.Serialize(out)
//	}
//	return nil
//}
//
//// Returns a representation of sql function call "func_call(c[0], ..., c[n-1])
//func SqlFunc(funcName string, expressions ...Expression) Expression {
//	f := &funcExpression{
//		funcName: funcName,
//	}
//	if len(expressions) > 0 {
//		args := make([]Clause, len(expressions), len(expressions))
//		for i, expr := range expressions {
//			args[i] = expr
//		}
//
//		f.args = &listClause{
//			clauses:            args,
//			includeParentheses: true,
//		}
//	}
//	return f
//}

type intervalExpression struct {
	expressionInterfaceImpl
	duration time.Duration
	negative bool
}

var intervalSep = ":"

func (c *intervalExpression) Serialize(out *queryData, options ...serializeOption) error {
	hours := c.duration / time.Hour
	minutes := (c.duration % time.Hour) / time.Minute
	sec := (c.duration % time.Minute) / time.Second
	msec := (c.duration % time.Second) / time.Microsecond
	out.WriteString("INTERVAL '")
	if c.negative {
		out.WriteString("-")
	}
	out.WriteString(strconv.FormatInt(int64(hours), 10))
	out.WriteString(intervalSep)
	out.WriteString(strconv.FormatInt(int64(minutes), 10))
	out.WriteString(intervalSep)
	out.WriteString(strconv.FormatInt(int64(sec), 10))
	out.WriteString(intervalSep)
	out.WriteString(strconv.FormatInt(int64(msec), 10))
	out.WriteString("' HOUR_MICROSECOND")

	return nil
}

// Interval returns a representation of duration
// in a form "INTERVAL `hour:min:sec:microsec` HOUR_MICROSECOND"
func Interval(duration time.Duration) Expression {
	negative := false
	if duration < 0 {
		negative = true
		duration = -duration
	}
	return &intervalExpression{
		duration: duration,
		negative: negative,
	}
}

var likeEscaper = strings.NewReplacer("_", "\\_", "%", "\\%")

func EscapeForLike(s string) string {
	return likeEscaper.Replace(s)
}

// Returns an escaped literal string
//func Literal(v interface{}) Expression {
//	value, err := sqltypes.BuildValue(v)
//	if err != nil {
//		panic(errors.Wrap(err, "Invalid literal value"))
//	}
//	return NewLiteralExpression(value)
//}
//
//// Returns a representation of "c[0] + ... + c[n-1]" for c in clauses
//func Add(expressions ...Expression) Expression {
//	return &arithmeticExpression{
//		expressions: expressions,
//		operator:    []byte(" + "),
//	}
//}
//
//// Returns a representation of "c[0] - ... - c[n-1]" for c in clauses
//func Sub(expressions ...Expression) Expression {
//	return &arithmeticExpression{
//		expressions: expressions,
//		operator:    []byte(" - "),
//	}
//}
//
//// Returns a representation of "c[0] * ... * c[n-1]" for c in clauses
//func Mul(expressions ...Expression) Expression {
//	return &arithmeticExpression{
//		expressions: expressions,
//		operator:    []byte(" * "),
//	}
//}
//
//// Returns a representation of "c[0] / ... / c[n-1]" for c in clauses
//func Div(expressions ...Expression) Expression {
//	return &arithmeticExpression{
//		expressions: expressions,
//		operator:    []byte(" / "),
//	}
//}

//TODO: Uncomment
//
//func BitOr(lhs, rhs Expression) Expression {
//	return &binaryExpression{
//		lhs:      lhs,
//		rhs:      rhs,
//		operator: []byte(" | "),
//	}
//}
//
//func BitAnd(lhs, rhs Expression) Expression {
//	return &binaryExpression{
//		lhs:      lhs,
//		rhs:      rhs,
//		operator: []byte(" & "),
//	}
//}
//
//func BitXor(lhs, rhs Expression) Expression {
//	return &binaryExpression{
//		lhs:      lhs,
//		rhs:      rhs,
//		operator: []byte(" ^ "),
//	}
//}
//
//func Plus(lhs, rhs Expression) Expression {
//	return &binaryExpression{
//		lhs:      lhs,
//		rhs:      rhs,
//		operator: []byte(" + "),
//	}
//}
//
//func Minus(lhs, rhs Expression) Expression {
//	return &binaryExpression{
//		lhs:      lhs,
//		rhs:      rhs,
//		operator: []byte(" - "),
//	}
//}

type ifExpression struct {
	expressionInterfaceImpl

	conditional     BoolExpression
	trueExpression  Expression
	falseExpression Expression
}

func (exp *ifExpression) Serialize(out *queryData, options ...serializeOption) error {
	out.WriteString("IF(")
	_ = exp.conditional.Serialize(out)
	out.WriteString(",")
	_ = exp.trueExpression.Serialize(out)
	out.WriteString(",")
	_ = exp.falseExpression.Serialize(out)
	out.WriteString(")")

	return nil
}

// Returns a representation of an if-expression, of the form:
//   IF (BOOLEAN TEST, VALUE-IF-TRUE, VALUE-IF-FALSE)
func If(conditional BoolExpression,
	trueExpression Expression,
	falseExpression Expression) Expression {
	return &ifExpression{
		conditional:     conditional,
		trueExpression:  trueExpression,
		falseExpression: falseExpression,
	}
}

//TODO: Uncomment
//type columnValueExpression struct {
//	isExpression
//	column NonAliasColumn
//}
//
//func ColumnValue(col NonAliasColumn) Expression {
//	return &columnValueExpression{
//		column: col,
//	}
//}
//
//func (cv *columnValueExpression) Serialize(out *bytes.Buffer) error {
//	_, _ = out.WriteString("VALUES(")
//	_ = cv.column.SerializeSqlForColumnList(out)
//	_ = out.WriteByte(')')
//	return nil
//}
