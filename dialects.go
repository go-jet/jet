package jet

import (
	"github.com/pkg/errors"
	"strconv"
)

var (
	Default    = MySQL
	PostgreSQL = newPostgresDialect()
	MySQL      = newMySQLDialect()
)

func newPostgresDialect() Dialect {
	postgresDialect := newDialect("PostgreSQL", "postgres")

	postgresDialect.CastOverride = postgresCAST
	postgresDialect.AliasQuoteChar = '"'
	postgresDialect.IdentifierQuoteChar = '"'
	postgresDialect.ArgumentPlaceholder = func(ord int) string {
		return "$" + strconv.Itoa(ord)
	}
	postgresDialect.SupportsReturning = true
	postgresDialect.UpdateAssigment = postgresUpdateAssigment

	return postgresDialect
}

func newMySQLDialect() Dialect {
	mySQLDialect := newDialect("MySQL", "mysql")

	mySQLDialect.SerializeOverrides["IS DISTINCT FROM"] = mysql_IS_DISTINCT_FROM
	mySQLDialect.SerializeOverrides["IS NOT DISTINCT FROM"] = mysql_IS_NOT_DISTINCT_FROM
	mySQLDialect.SerializeOverrides["/"] = mysql_DIVISION
	mySQLDialect.SerializeOverrides["#"] = mysql_BIT_XOR

	mySQLDialect.AliasQuoteChar = '"'
	mySQLDialect.IdentifierQuoteChar = '`'
	mySQLDialect.ArgumentPlaceholder = func(int) string {
		return "?"
	}

	mySQLDialect.SupportsReturning = false
	mySQLDialect.UpdateAssigment = mysqlUpdateAssigment

	return mySQLDialect
}

type Dialect struct {
	Name                string
	PackageName         string
	SerializeOverrides  map[string]serializeOverride
	CastOverride        castOverride
	AliasQuoteChar      byte
	IdentifierQuoteChar byte
	ArgumentPlaceholder queryPlaceholderFunc
	UpdateAssigment     func(columns []column, values []clause, out *sqlBuilder) (err error)

	SupportsReturning bool
}

func (d *Dialect) serializeOverride(operator string) serializeOverride {
	return d.SerializeOverrides[operator]
}

func mysqlUpdateAssigment(columns []column, values []clause, out *sqlBuilder) (err error) {

	if len(columns) != len(values) {
		return errors.New("jet: mismatch in numers of columns and values")
	}

	for i, column := range columns {
		if i > 0 {
			out.writeString(", ")
		}

		out.writeString(column.Name())

		out.writeString(" = ")

		if err = values[i].serialize(updateStatement, out); err != nil {
			return err
		}
	}

	return nil
}

func postgresUpdateAssigment(columns []column, values []clause, out *sqlBuilder) (err error) {
	if len(columns) > 1 {
		out.writeString("(")
	}

	err = serializeColumnNames(columns, out)

	if err != nil {
		return
	}

	if len(columns) > 1 {
		out.writeString(")")
	}

	out.writeString("=")

	if len(values) > 1 {
		out.writeString("(")
	}

	err = serializeClauseList(updateStatement, values, out)

	if err != nil {
		return
	}

	if len(values) > 1 {
		out.writeString(")")
	}

	return
}

type queryPlaceholderFunc func(ord int) string

func newDialect(name, packageName string) Dialect {
	newDialect := Dialect{
		Name:        name,
		PackageName: packageName,
	}
	newDialect.SerializeOverrides = make(map[string]serializeOverride)

	return newDialect
}

func mysql_BIT_XOR(expressions ...Expression) serializeFunc {
	return func(statement statementType, out *sqlBuilder, options ...serializeOption) error {
		if len(expressions) != 2 {
			return errors.New("Invalid number of expressions for operator")
		}

		lhs := expressions[0]
		rhs := expressions[1]

		if err := lhs.serialize(statement, out, options...); err != nil {
			return err
		}

		out.writeString("^")

		if err := rhs.serialize(statement, out, options...); err != nil {
			return err
		}
		return nil
	}
}

func mysql_DIVISION(expressions ...Expression) serializeFunc {
	return func(statement statementType, out *sqlBuilder, options ...serializeOption) error {
		if len(expressions) != 2 {
			return errors.New("Invalid number of expressions for operator")
		}

		lhs := expressions[0]
		rhs := expressions[1]

		if err := lhs.serialize(statement, out, options...); err != nil {
			return err
		}

		_, isLhsInt := lhs.(IntegerExpression)
		_, isRhsInt := rhs.(IntegerExpression)

		if isLhsInt && isRhsInt {
			out.writeString("DIV")
		} else {
			out.writeString("/")
		}

		if err := rhs.serialize(statement, out, options...); err != nil {
			return err
		}
		return nil
	}
}

func mysql_IS_NOT_DISTINCT_FROM(expressions ...Expression) serializeFunc {
	return func(statement statementType, out *sqlBuilder, options ...serializeOption) error {
		if len(expressions) != 2 {
			return errors.New("Invalid number of expressions for operator")
		}
		if err := expressions[0].serialize(statement, out); err != nil {
			return err
		}

		out.writeString("<=>")

		if err := expressions[1].serialize(statement, out); err != nil {
			return err
		}

		return nil
	}
}

func mysql_IS_DISTINCT_FROM(expressions ...Expression) serializeFunc {
	return func(statement statementType, out *sqlBuilder, options ...serializeOption) error {
		out.writeString("NOT")

		err := mysql_IS_NOT_DISTINCT_FROM(expressions...)(statement, out, options...)

		if err != nil {
			return err
		}

		return nil
	}
}

func postgresCAST(expression Expression, castType string) serializeFunc {
	return func(statement statementType, out *sqlBuilder, options ...serializeOption) error {
		if err := expression.serialize(statement, out, options...); err != nil {
			return err
		}
		out.writeString("::" + castType)
		return nil
	}
}

type serializeFunc func(statement statementType, out *sqlBuilder, options ...serializeOption) error
type serializeOverride func(expressions ...Expression) serializeFunc

type castOverride func(expression Expression, castType string) serializeFunc
