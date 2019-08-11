package mysql

import (
	"errors"
	"github.com/go-jet/jet/internal/jet"
)

var Dialect = NewDialect()

func NewDialect() jet.Dialect {

	serializeOverrides := map[string]jet.SerializeOverride{}
	serializeOverrides["IS DISTINCT FROM"] = mysql_IS_DISTINCT_FROM
	serializeOverrides["IS NOT DISTINCT FROM"] = mysql_IS_NOT_DISTINCT_FROM
	serializeOverrides["/"] = mysql_DIVISION
	serializeOverrides["#"] = mysql_BIT_XOR
	serializeOverrides[jet.StringConcatOperator] = mysql_CONCAT_operator

	mySQLDialectParams := jet.DialectParams{
		Name:                "MySQL",
		PackageName:         "mysql",
		SerializeOverrides:  serializeOverrides,
		AliasQuoteChar:      '"',
		IdentifierQuoteChar: '`',
		ArgumentPlaceholder: func(int) string {
			return "?"
		},
	}

	return jet.NewDialect(mySQLDialectParams)
}

func mysql_BIT_XOR(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) error {
		if len(expressions) != 2 {
			return errors.New("jet: invalid number of expressions for operator")
		}

		lhs := expressions[0]
		rhs := expressions[1]

		if err := jet.Serialize(lhs, statement, out, options...); err != nil {
			return err
		}

		out.WriteString("^")

		if err := jet.Serialize(rhs, statement, out, options...); err != nil {
			return err
		}
		return nil
	}
}

func mysql_CONCAT_operator(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) error {
		if len(expressions) != 2 {
			return errors.New("jet: invalid number of expressions for operator")
		}

		out.WriteString("CONCAT(")

		if err := jet.Serialize(expressions[0], statement, out, options...); err != nil {
			return err
		}

		out.WriteString(", ")

		if err := jet.Serialize(expressions[1], statement, out, options...); err != nil {
			return err
		}

		out.WriteString(")")

		return nil
	}
}

func mysql_DIVISION(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) error {
		if len(expressions) != 2 {
			return errors.New("jet: invalid number of expressions for operator")
		}

		lhs := expressions[0]
		rhs := expressions[1]

		if err := jet.Serialize(lhs, statement, out, options...); err != nil {
			return err
		}

		_, isLhsInt := lhs.(IntegerExpression)
		_, isRhsInt := rhs.(IntegerExpression)

		if isLhsInt && isRhsInt {
			out.WriteString("DIV")
		} else {
			out.WriteString("/")
		}

		if err := jet.Serialize(rhs, statement, out, options...); err != nil {
			return err
		}
		return nil
	}
}

func mysql_IS_NOT_DISTINCT_FROM(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) error {
		if len(expressions) != 2 {
			return errors.New("jet: invalid number of expressions for operator")
		}
		if err := jet.Serialize(expressions[0], statement, out); err != nil {
			return err
		}

		out.WriteString("<=>")

		if err := jet.Serialize(expressions[1], statement, out); err != nil {
			return err
		}

		return nil
	}
}

func mysql_IS_DISTINCT_FROM(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) error {
		out.WriteString("NOT(")

		err := mysql_IS_NOT_DISTINCT_FROM(expressions...)(statement, out, options...)

		if err != nil {
			return err
		}

		out.WriteString(")")

		return nil
	}
}
