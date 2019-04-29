package sqlbuilder

import (
	"github.com/dropbox/godropbox/errors"
)

// An expression
type Expression interface {
	Clause
	Projection

	As(alias string) Projection
	IsDistinct(expression Expression) BoolExpression
	IsNull() BoolExpression
	Asc() OrderByClause
	Desc() OrderByClause
}

type expressionInterfaceImpl struct {
	parent Expression
}

func (e *expressionInterfaceImpl) As(alias string) Projection {
	return NewAlias(e.parent, alias)
}

func (e *expressionInterfaceImpl) IsDistinct(expression Expression) BoolExpression {
	return nil
}

func (e *expressionInterfaceImpl) IsNull() BoolExpression {
	return nil
}

func (e *expressionInterfaceImpl) Asc() OrderByClause {
	return &orderByClause{expression: e.parent, ascent: true}
}

func (e *expressionInterfaceImpl) Desc() OrderByClause {
	return &orderByClause{expression: e.parent, ascent: false}
}

func (e *expressionInterfaceImpl) SerializeForProjection(out *queryData) error {
	return e.parent.Serialize(out, FOR_PROJECTION)
}

// Representation of binary operations (e.g. comparisons, arithmetic)
type binaryExpression struct {
	lhs, rhs Expression
	operator []byte
}

func newBinaryExpression(lhs, rhs Expression, operator []byte, parent ...Expression) binaryExpression {
	binaryExpression := binaryExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: operator,
	}

	return binaryExpression
}

func (c *binaryExpression) Serialize(out *queryData, options ...serializeOption) error {
	if c.lhs == nil {
		return errors.Newf("nil lhs.")
	}
	if err := c.lhs.Serialize(out); err != nil {
		return err
	}

	out.Write(c.operator)

	if c.rhs == nil {
		return errors.Newf("nil rhs.")
	}
	if err := c.rhs.Serialize(out); err != nil {
		return err
	}

	return nil
}

// A not expression which negates a expression value
type prefixExpression struct {
	expression Expression
	operator   []byte
}

func newPrefixExpression(expression Expression, operator []byte) prefixExpression {
	prefixExpression := prefixExpression{
		expression: expression,
		operator:   operator,
	}

	return prefixExpression
}

func (p *prefixExpression) Serialize(out *queryData, options ...serializeOption) error {
	out.Write(p.operator)

	if p.expression == nil {
		return errors.Newf("nil prefix expression.")
	}
	if err := p.expression.Serialize(out); err != nil {
		return err
	}

	return nil
}

//
//// Representation of n-ary conjunctions (AND/OR)
//type conjunctExpression struct {
//	expressions []Expression
//	conjunction []byte
//}
//
//func (conj *conjunctExpression) Serialize(out *queryData, options ...serializeOption) error {
//	if len(conj.expressions) == 0 {
//		return errors.New("Empty conjunction.")
//	}
//
//	//clauses := make([]Clause, len(conj.expressions), len(conj.expressions))
//	//for i, expr := range conj.expressions {
//	//	clauses[i] = expr
//	//}
//
//	useParentheses := len(conj.expressions) > 1
//	if useParentheses {
//		out.WriteByte('(')
//	}
//
//	if err := serializeExpressionList(conj.expressions, string(conj.conjunction), out); err != nil {
//		return err
//	}
//
//	if useParentheses {
//		out.WriteByte(')')
//	}
//
//	return nil
//}

//--------------------------------------------------------------

//------------------------------------------------------//
//// Dummy type for select *
//type ColumnList []Column
//
//func (cl ColumnList) Serialize(out *bytes.Buffer, options ...serializeOption) error {
//	for i, column := range cl {
//		err := column.Serialize(out)
//
//		if err != nil {
//			return err
//		}
//
//		if i != len(cl)-1 {
//			out.WriteString(", ")
//		}
//	}
//	return nil
//}
//
//func (e ColumnList) As(alias string) Clause {
//	panic("Invalid usage")
//}
//
//func (e ColumnList) IsDistinct(expression Expression) BoolExpression {
//	panic("Invalid usage")
//}
//
//func (e ColumnList) IsNull(expression Expression) BoolExpression {
//	panic("Invalid usage")
//}
