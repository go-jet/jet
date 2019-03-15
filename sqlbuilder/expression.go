// Query building functions for expression components
package sqlbuilder

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/dropbox/godropbox/database/sqltypes"
	"github.com/dropbox/godropbox/errors"
)

type orderByClause struct {
	isOrderByClause
	expression Expression
	ascent     bool
}

func (o *orderByClause) SerializeSql(out *bytes.Buffer) error {
	if o.expression == nil {
		return errors.Newf(
			"nil order by clause.  Generated sql: %s",
			out.String())
	}

	if err := o.expression.SerializeSql(out); err != nil {
		return err
	}

	if o.ascent {
		_, _ = out.WriteString(" ASC")
	} else {
		_, _ = out.WriteString(" DESC")
	}

	return nil
}

func Asc(expression Expression) OrderByClause {
	return &orderByClause{expression: expression, ascent: true}
}

func Desc(expression Expression) OrderByClause {
	return &orderByClause{expression: expression, ascent: false}
}

// Representation of an escaped literal
type literalExpression struct {
	isExpression
	value sqltypes.Value
}

func (c literalExpression) SerializeSql(out *bytes.Buffer) error {
	sqltypes.Value(c.value).EncodeSql(out)
	return nil
}

func serializeClauses(
	clauses []Clause,
	separator []byte,
	out *bytes.Buffer) (err error) {

	if clauses == nil || len(clauses) == 0 {
		return errors.Newf("Empty clauses.  Generated sql: %s", out.String())
	}

	if clauses[0] == nil {
		return errors.Newf("nil clause.  Generated sql: %s", out.String())
	}
	if err = clauses[0].SerializeSql(out); err != nil {
		return
	}

	for _, c := range clauses[1:] {
		_, _ = out.Write(separator)

		if c == nil {
			return errors.Newf("nil clause.  Generated sql: %s", out.String())
		}
		if err = c.SerializeSql(out); err != nil {
			return
		}
	}

	return nil
}

// Representation of n-ary arithmetic (+ - * /)
type arithmeticExpression struct {
	isExpression
	expressions []Expression
	operator    []byte
}

func (arith *arithmeticExpression) SerializeSql(out *bytes.Buffer) (err error) {
	if len(arith.expressions) == 0 {
		return errors.Newf(
			"Empty arithmetic expression.  Generated sql: %s",
			out.String())
	}

	clauses := make([]Clause, len(arith.expressions), len(arith.expressions))
	for i, expr := range arith.expressions {
		clauses[i] = expr
	}

	useParentheses := len(clauses) > 1
	if useParentheses {
		_ = out.WriteByte('(')
	}

	if err = serializeClauses(clauses, arith.operator, out); err != nil {
		return
	}

	if useParentheses {
		_ = out.WriteByte(')')
	}

	return nil
}

type tupleExpression struct {
	isExpression
	elements listClause
}

func (tuple *tupleExpression) SerializeSql(out *bytes.Buffer) error {
	if len(tuple.elements.clauses) < 1 {
		return errors.Newf("Tuples must include at least one element")
	}
	return tuple.elements.SerializeSql(out)
}

func Tuple(exprs ...Expression) Expression {
	clauses := make([]Clause, 0, len(exprs))
	for _, expr := range exprs {
		clauses = append(clauses, expr)
	}
	return &tupleExpression{
		elements: listClause{
			clauses:            clauses,
			includeParentheses: true,
		},
	}
}

// Representation of a tuple enclosed, comma separated list of clauses
type listClause struct {
	clauses            []Clause
	includeParentheses bool
}

func (list *listClause) SerializeSql(out *bytes.Buffer) error {
	if list.includeParentheses {
		_ = out.WriteByte('(')
	}

	if err := serializeClauses(list.clauses, []byte(","), out); err != nil {
		return err
	}

	if list.includeParentheses {
		_ = out.WriteByte(')')
	}
	return nil
}

// Representation of binary operations (e.g. comparisons, arithmetic)
type binaryExpression struct {
	isExpression
	lhs, rhs Expression
	operator []byte
}

func (c *binaryExpression) SerializeSql(out *bytes.Buffer) (err error) {
	if c.lhs == nil {
		return errors.Newf("nil lhs.  Generated sql: %s", out.String())
	}
	if err = c.lhs.SerializeSql(out); err != nil {
		return
	}

	_, _ = out.Write(c.operator)

	if c.rhs == nil {
		return errors.Newf("nil rhs.  Generated sql: %s", out.String())
	}
	if err = c.rhs.SerializeSql(out); err != nil {
		return
	}

	return nil
}

type funcExpression struct {
	isExpression
	funcName string
	args     *listClause
}

func (c *funcExpression) SerializeSql(out *bytes.Buffer) (err error) {
	if !validIdentifierName(c.funcName) {
		return errors.Newf(
			"Invalid function name: %s.  Generated sql: %s",
			c.funcName,
			out.String())
	}
	_, _ = out.WriteString(c.funcName)
	if c.args == nil {
		_, _ = out.WriteString("()")
	} else {
		return c.args.SerializeSql(out)
	}
	return nil
}

// Returns a representation of sql function call "func_call(c[0], ..., c[n-1])
func SqlFunc(funcName string, expressions ...Expression) Expression {
	f := &funcExpression{
		funcName: funcName,
	}
	if len(expressions) > 0 {
		args := make([]Clause, len(expressions), len(expressions))
		for i, expr := range expressions {
			args[i] = expr
		}

		f.args = &listClause{
			clauses:            args,
			includeParentheses: true,
		}
	}
	return f
}

type intervalExpression struct {
	isExpression
	duration time.Duration
	negative bool
}

var intervalSep = ":"

func (c *intervalExpression) SerializeSql(out *bytes.Buffer) (err error) {
	hours := c.duration / time.Hour
	minutes := (c.duration % time.Hour) / time.Minute
	sec := (c.duration % time.Minute) / time.Second
	msec := (c.duration % time.Second) / time.Microsecond
	_, _ = out.WriteString("INTERVAL '")
	if c.negative {
		_, _ = out.WriteString("-")
	}
	_, _ = out.WriteString(strconv.FormatInt(int64(hours), 10))
	_, _ = out.WriteString(intervalSep)
	_, _ = out.WriteString(strconv.FormatInt(int64(minutes), 10))
	_, _ = out.WriteString(intervalSep)
	_, _ = out.WriteString(strconv.FormatInt(int64(sec), 10))
	_, _ = out.WriteString(intervalSep)
	_, _ = out.WriteString(strconv.FormatInt(int64(msec), 10))
	_, _ = out.WriteString("' HOUR_MICROSECOND")
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
func Literal(v interface{}) Expression {
	value, err := sqltypes.BuildValue(v)
	if err != nil {
		panic(errors.Wrap(err, "Invalid literal value"))
	}
	return &literalExpression{value: value}
}

// Returns a representation of "c[0] + ... + c[n-1]" for c in clauses
func Add(expressions ...Expression) Expression {
	return &arithmeticExpression{
		expressions: expressions,
		operator:    []byte(" + "),
	}
}

// Returns a representation of "c[0] - ... - c[n-1]" for c in clauses
func Sub(expressions ...Expression) Expression {
	return &arithmeticExpression{
		expressions: expressions,
		operator:    []byte(" - "),
	}
}

// Returns a representation of "c[0] * ... * c[n-1]" for c in clauses
func Mul(expressions ...Expression) Expression {
	return &arithmeticExpression{
		expressions: expressions,
		operator:    []byte(" * "),
	}
}

// Returns a representation of "c[0] / ... / c[n-1]" for c in clauses
func Div(expressions ...Expression) Expression {
	return &arithmeticExpression{
		expressions: expressions,
		operator:    []byte(" / "),
	}
}

func BitOr(lhs, rhs Expression) Expression {
	return &binaryExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: []byte(" | "),
	}
}

func BitAnd(lhs, rhs Expression) Expression {
	return &binaryExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: []byte(" & "),
	}
}

func BitXor(lhs, rhs Expression) Expression {
	return &binaryExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: []byte(" ^ "),
	}
}

func Plus(lhs, rhs Expression) Expression {
	return &binaryExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: []byte(" + "),
	}
}

func Minus(lhs, rhs Expression) Expression {
	return &binaryExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: []byte(" - "),
	}
}

type ifExpression struct {
	isExpression
	conditional     BoolExpression
	trueExpression  Expression
	falseExpression Expression
}

func (exp *ifExpression) SerializeSql(out *bytes.Buffer) error {
	_, _ = out.WriteString("IF(")
	_ = exp.conditional.SerializeSql(out)
	_, _ = out.WriteString(",")
	_ = exp.trueExpression.SerializeSql(out)
	_, _ = out.WriteString(",")
	_ = exp.falseExpression.SerializeSql(out)
	_, _ = out.WriteString(")")
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

type columnValueExpression struct {
	isExpression
	column NonAliasColumn
}

func ColumnValue(col NonAliasColumn) Expression {
	return &columnValueExpression{
		column: col,
	}
}

func (cv *columnValueExpression) SerializeSql(out *bytes.Buffer) error {
	_, _ = out.WriteString("VALUES(")
	_ = cv.column.SerializeSqlForColumnList(out)
	_ = out.WriteByte(')')
	return nil
}
