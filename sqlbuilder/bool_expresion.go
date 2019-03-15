package sqlbuilder

import (
	"bytes"
	"github.com/dropbox/godropbox/database/sqltypes"
	"github.com/dropbox/godropbox/errors"
	"reflect"
	"time"
)

// Returns a representation of "a=b"
func Eq(lhs, rhs Expression) BoolExpression {
	lit, ok := rhs.(*literalExpression)
	if ok && sqltypes.Value(lit.value).IsNull() {
		return newBoolExpression(lhs, rhs, []byte(" IS "))
	}
	return newBoolExpression(lhs, rhs, []byte(" = "))
}

// Returns a representation of "a=b", where b is a literal
func EqL(lhs Expression, val interface{}) BoolExpression {
	return Eq(lhs, Literal(val))
}

// Returns a representation of "a!=b"
func Neq(lhs, rhs Expression) BoolExpression {
	lit, ok := rhs.(*literalExpression)
	if ok && sqltypes.Value(lit.value).IsNull() {
		return newBoolExpression(lhs, rhs, []byte(" IS NOT "))
	}
	return newBoolExpression(lhs, rhs, []byte("!="))
}

// Returns a representation of "a!=b", where b is a literal
func NeqL(lhs Expression, val interface{}) BoolExpression {
	return Neq(lhs, Literal(val))
}

// Returns a representation of "a<b"
func Lt(lhs Expression, rhs Expression) BoolExpression {
	return newBoolExpression(lhs, rhs, []byte("<"))
}

// Returns a representation of "a<b", where b is a literal
func LtL(lhs Expression, val interface{}) BoolExpression {
	return Lt(lhs, Literal(val))
}

// Returns a representation of "a<=b"
func Lte(lhs, rhs Expression) BoolExpression {
	return newBoolExpression(lhs, rhs, []byte("<="))
}

// Returns a representation of "a<=b", where b is a literal
func LteL(lhs Expression, val interface{}) BoolExpression {
	return Lte(lhs, Literal(val))
}

// Returns a representation of "a>b"
func Gt(lhs, rhs Expression) BoolExpression {
	return newBoolExpression(lhs, rhs, []byte(">"))
}

// Returns a representation of "a>b", where b is a literal
func GtL(lhs Expression, val interface{}) BoolExpression {
	return Gt(lhs, Literal(val))
}

// Returns a representation of "a>=b"
func Gte(lhs, rhs Expression) BoolExpression {
	return newBoolExpression(lhs, rhs, []byte(">="))
}

// Returns a representation of "a>=b", where b is a literal
func GteL(lhs Expression, val interface{}) BoolExpression {
	return Gte(lhs, Literal(val))
}

// Returns a representation of "not expr"
func Not(expr BoolExpression) BoolExpression {
	return &negateExpression{
		nested: expr,
	}
}

// Returns a representation of "c[0] AND ... AND c[n-1]" for c in clauses
func And(expressions ...BoolExpression) BoolExpression {
	return &conjunctExpression{
		expressions: expressions,
		conjunction: []byte(" AND "),
	}
}

// Returns a representation of "c[0] OR ... OR c[n-1]" for c in clauses
func Or(expressions ...BoolExpression) BoolExpression {
	return &conjunctExpression{
		expressions: expressions,
		conjunction: []byte(" OR "),
	}
}

func Like(lhs, rhs Expression) BoolExpression {
	return newBoolExpression(lhs, rhs, []byte(" LIKE "))
}

func LikeL(lhs Expression, val string) BoolExpression {
	return Like(lhs, Literal(val))
}

func Regexp(lhs, rhs Expression) BoolExpression {
	return newBoolExpression(lhs, rhs, []byte(" REGEXP "))
}

func RegexpL(lhs Expression, val string) BoolExpression {
	return Regexp(lhs, Literal(val))
}

// Returns a representation of "a IN (b[0], ..., b[n-1])", where b is a list
// of literals valList must be a slice type
func In(lhs Expression, valList interface{}) BoolExpression {
	var clauses []Clause
	switch val := valList.(type) {
	// This atrocious body of copy-paste code is due to the fact that if you
	// try to merge the cases, you can't treat val as a list
	case []int:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []int32:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []int64:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []uint:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []uint32:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []uint64:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []float64:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []string:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case [][]byte:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []time.Time:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []sqltypes.Numeric:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []sqltypes.Fractional:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []sqltypes.String:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	case []sqltypes.Value:
		clauses = make([]Clause, 0, len(val))
		for _, v := range val {
			clauses = append(clauses, Literal(v))
		}
	default:
		return &inExpression{
			err: errors.Newf(
				"Unknown value list type in IN clause: %s",
				reflect.TypeOf(valList)),
		}
	}

	expr := &inExpression{lhs: lhs}
	if len(clauses) > 0 {
		expr.rhs = &listClause{clauses: clauses, includeParentheses: true}
	}
	return expr
}

type boolExpressionImpl struct {
	isExpression
	isBoolExpression
}

func (c *boolExpressionImpl) And(expression BoolExpression) BoolExpression {
	return And(c, expression)
}

func (c *boolExpressionImpl) Or(expression BoolExpression) BoolExpression {
	return Or(c, expression)
}

func (conj *boolExpressionImpl) SerializeSql(out *bytes.Buffer) (err error) {
	return errors.New("Not implemented")
}

// Representation of n-ary conjunctions (AND/OR)
type conjunctExpression struct {
	boolExpressionImpl
	expressions []BoolExpression
	conjunction []byte
}

func (conj *conjunctExpression) SerializeSql(out *bytes.Buffer) (err error) {
	if len(conj.expressions) == 0 {
		return errors.Newf(
			"Empty conjunction.  Generated sql: %s",
			out.String())
	}

	clauses := make([]Clause, len(conj.expressions), len(conj.expressions))
	for i, expr := range conj.expressions {
		clauses[i] = expr
	}

	useParentheses := len(clauses) > 1
	if useParentheses {
		_ = out.WriteByte('(')
	}

	if err = serializeClauses(clauses, conj.conjunction, out); err != nil {
		return
	}

	if useParentheses {
		_ = out.WriteByte(')')
	}

	return nil
}

// A not expression which negates a expression value
type negateExpression struct {
	boolExpressionImpl

	nested BoolExpression
}

func (c *negateExpression) SerializeSql(out *bytes.Buffer) (err error) {
	_, _ = out.WriteString("NOT (")

	if c.nested == nil {
		return errors.Newf("nil nested.  Generated sql: %s", out.String())
	}
	if err = c.nested.SerializeSql(out); err != nil {
		return
	}

	_ = out.WriteByte(')')
	return nil
}

// A binary expression that evaluates to a boolean value.
type boolBinaryExpression struct {
	boolExpressionImpl
	binaryExpression binaryExpression
}

func (b *boolBinaryExpression) And(expression BoolExpression) BoolExpression {
	return And(b, expression)
}

func newBoolExpression(lhs, rhs Expression, operator []byte) *boolBinaryExpression {
	// go does not allow {} syntax for initializing promoted fields ...
	expr := new(boolBinaryExpression)
	expr.binaryExpression.lhs = lhs
	expr.binaryExpression.rhs = rhs
	expr.binaryExpression.operator = operator
	return expr
}

func (b *boolBinaryExpression) SerializeSql(out *bytes.Buffer) (err error) {
	return b.binaryExpression.SerializeSql(out)
}

// in expression representation
type inExpression struct {
	boolExpressionImpl

	lhs Expression
	rhs *listClause

	err error
}

func (c *inExpression) SerializeSql(out *bytes.Buffer) error {
	if c.err != nil {
		return errors.Wrap(c.err, "Invalid IN expression")
	}

	if c.lhs == nil {
		return errors.Newf(
			"lhs of in expression is nil.  Generated sql: %s",
			out.String())
	}

	// We'll serialize the lhs even if we don't need it to ensure no error
	buf := &bytes.Buffer{}

	err := c.lhs.SerializeSql(buf)
	if err != nil {
		return err
	}

	if c.rhs == nil {
		_, _ = out.WriteString("FALSE")
		return nil
	}

	_, _ = out.WriteString(buf.String())
	_, _ = out.WriteString(" IN ")

	err = c.rhs.SerializeSql(out)
	if err != nil {
		return err
	}

	return nil
}
