package sqlbuilder

import (
	"bytes"
	"github.com/dropbox/godropbox/database/sqltypes"
	"github.com/dropbox/godropbox/errors"
	"reflect"
	"time"
)

type BoolExpression interface {
	Expression

	Eq(expression BoolExpression) BoolExpression
	NotEq(expression BoolExpression) BoolExpression
	GtEq(rhs Expression) BoolExpression
	LtEq(rhs Expression) BoolExpression

	And(expression BoolExpression) BoolExpression
	Or(expression BoolExpression) BoolExpression
	IsTrue() BoolExpression
	IsFalse() BoolExpression
}

type boolInterfaceImpl struct {
	parent BoolExpression
}

func (b *boolInterfaceImpl) Eq(expression BoolExpression) BoolExpression {
	return Eq(b.parent, expression)
}

func (b *boolInterfaceImpl) NotEq(expression BoolExpression) BoolExpression {
	return NotEq(b.parent, expression)
}

func (b *boolInterfaceImpl) GtEq(rhs Expression) BoolExpression {
	return GtEq(b.parent, rhs)
}

func (b *boolInterfaceImpl) LtEq(rhs Expression) BoolExpression {
	return LtEq(b.parent, rhs)
}

func (b *boolInterfaceImpl) And(expression BoolExpression) BoolExpression {
	return And(b.parent, expression)
}

func (b *boolInterfaceImpl) Or(expression BoolExpression) BoolExpression {
	return Or(b.parent, expression)
}
func (b *boolInterfaceImpl) IsTrue() BoolExpression {
	return IsTrue(b.parent)
}

func (b *boolInterfaceImpl) IsFalse() BoolExpression {
	return nil
}

//---------------------------------------------------//
type boolLiteralExpression struct {
	boolInterfaceImpl
	literalExpression
}

func newBoolLiteralExpression(value bool) BoolExpression {
	boolLiteralExpression := boolLiteralExpression{}

	boolLiteralExpression.literalExpression = *Literal(value)
	boolLiteralExpression.boolInterfaceImpl.parent = &boolLiteralExpression

	return &boolLiteralExpression
}

//---------------------------------------------------//
type binaryBoolExpression struct {
	expressionInterfaceImpl
	boolInterfaceImpl

	binaryExpression
}

func newBinaryBoolExpression(lhs, rhs Expression, operator []byte) BoolExpression {
	boolExpression := binaryBoolExpression{}

	boolExpression.binaryExpression = newBinaryExpression(lhs, rhs, operator)
	boolExpression.expressionInterfaceImpl.parent = &boolExpression
	boolExpression.boolInterfaceImpl.parent = &boolExpression

	return &boolExpression
}

//---------------------------------------------------//
type prefixBoolExpression struct {
	expressionInterfaceImpl
	boolInterfaceImpl

	prefixExpression
}

func newPrefixBoolExpression(expression Expression, operator []byte) BoolExpression {
	boolExpression := prefixBoolExpression{}
	boolExpression.prefixExpression = newPrefixExpression(expression, operator)

	boolExpression.expressionInterfaceImpl.parent = &boolExpression
	boolExpression.boolInterfaceImpl.parent = &boolExpression

	return &boolExpression
}

//---------------------------------------------------//
//type conjunctBoolExpression struct {
//	expressionInterfaceImpl
//	boolInterfaceImpl
//
//	conjunctExpression
//	name string
//}
//
//func NewConjunctBoolExpression(operator []byte, expressions ...BoolExpression) BoolExpression {
//	boolExpression := conjunctBoolExpression{
//		conjunctExpression: conjunctExpression{
//			expressions: expressions,
//			conjunction: operator,
//		},
//	}
//
//	boolExpression.expressionInterfaceImpl.parent = &boolExpression
//	boolExpression.boolInterfaceImpl.parent = &boolExpression
//
//	return &boolExpression
//}

//---------------------------------------------------//
type inExpression struct {
	expressionInterfaceImpl
	boolInterfaceImpl

	lhs Expression
	rhs *listClause

	err error
}

func (c *inExpression) Serialize(out *queryData, options ...serializeOption) error {
	if c.err != nil {
		return errors.Wrap(c.err, "Invalid IN expression")
	}

	if c.lhs == nil {
		return errors.Newf("lhs of in expression is nil.")
	}

	// We'll serialize the lhs even if we don't need it to ensure no error
	buf := &bytes.Buffer{}

	err := c.lhs.Serialize(out, options...)
	if err != nil {
		return err
	}

	if c.rhs == nil {
		out.WriteString("FALSE")
		return nil
	}

	out.WriteString(buf.String())
	out.WriteString(" IN ")

	err = c.rhs.Serialize(out)

	if err != nil {
		return err
	}

	return nil
}

// Returns a representation of "a=b"
func Eq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, []byte(" = "))
}

// Returns a representation of "a=b", where b is a literal
func EqL(lhs Expression, val interface{}) BoolExpression {
	return Eq(lhs, Literal(val))
}

// Returns a representation of "a!=b"
func NotEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, []byte("!="))
}

// Returns a representation of "a!=b", where b is a literal
func NeqL(lhs Expression, val interface{}) BoolExpression {
	return NotEq(lhs, Literal(val))
}

// Returns a representation of "a<b"
func Lt(lhs Expression, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, []byte("<"))
}

// Returns a representation of "a<b", where b is a literal
func LtL(lhs Expression, val interface{}) BoolExpression {
	return Lt(lhs, Literal(val))
}

// Returns a representation of "a<=b"
func LtEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, []byte("<="))
}

// Returns a representation of "a<=b", where b is a literal
func LteL(lhs Expression, val interface{}) BoolExpression {
	return LtEq(lhs, Literal(val))
}

// Returns a representation of "a>b"
func Gt(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, []byte(">"))
}

// Returns a representation of "a>b", where b is a literal
func GtL(lhs Expression, val interface{}) BoolExpression {
	return Gt(lhs, Literal(val))
}

// Returns a representation of "a>=b"
func GtEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, []byte(">="))
}

// Returns a representation of "a>=b", where b is a literal
func GteL(lhs Expression, val interface{}) BoolExpression {
	return GtEq(lhs, Literal(val))
}

// Returns a representation of "not expr"
func Not(expr BoolExpression) BoolExpression {
	return newPrefixBoolExpression(expr, []byte(" NOT "))
}

func IsTrue(expr BoolExpression) BoolExpression {
	return newPrefixBoolExpression(expr, []byte(" IS TRUE "))
}

func And(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, []byte(" AND "))
}

// Returns a representation of "c[0] OR ... OR c[n-1]" for c in clauses
func Or(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, []byte(" OR "))
}

func Like(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, []byte(" LIKE "))
}

func LikeL(lhs Expression, val string) BoolExpression {
	return Like(lhs, Literal(val))
}

func Regexp(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, []byte(" REGEXP "))
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
