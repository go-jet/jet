package jet

import (
	"github.com/pkg/errors"
	"strconv"
)

var (
	PostgreSQL = newPostgresDialect()
	MySQL      = newMySQLDialect()
)

func newPostgresDialect() Dialect {
	postgresDialect := newDialect("PostgreSQL", "postgres")

	postgresDialect.OperatorOverrides["IS DISTINCT FROM"] = postgresIS_DISTINCT_FROM
	postgresDialect.CastOverride = postgresCAST
	postgresDialect.AliasQuoteChar = '"'
	postgresDialect.IdentifierQuoteChar = '"'
	postgresDialect.ArgumentPlaceholder = func(ord int) string {
		return "$" + strconv.Itoa(ord)
	}

	return postgresDialect
}

func newMySQLDialect() Dialect {
	mySQLDialect := newDialect("MySQL", "mysql")

	mySQLDialect.AliasQuoteChar = '"'
	mySQLDialect.IdentifierQuoteChar = '"'
	mySQLDialect.ArgumentPlaceholder = func(int) string {
		return "?"
	}

	return mySQLDialect
}

type Dialect struct {
	Name                string
	PackageName         string
	OperatorOverrides   map[string]serializeOverride
	CastOverride        castOverride
	AliasQuoteChar      byte
	IdentifierQuoteChar byte
	ArgumentPlaceholder queryPlaceholderFunc
}

func (d *Dialect) serializeOverride(operator string) serializeOverride {
	return d.OperatorOverrides[operator]
}

type queryPlaceholderFunc func(ord int) string

func newDialect(name, packageName string) Dialect {
	newDialect := Dialect{
		Name:        name,
		PackageName: packageName,
	}
	newDialect.OperatorOverrides = make(map[string]serializeOverride)

	return newDialect
}

func postgresIS_DISTINCT_FROM(expressions ...Expression) serializeFunc {
	return func(statement statementType, out *sqlBuilder, options ...serializeOption) error {
		if len(expressions) != 2 {
			return errors.New("Invalid number of expressions for operator")
		}
		if err := expressions[0].serialize(statement, out); err != nil {
			return err
		}

		out.writeString("IS DISTINCT FROM")

		if err := expressions[1].serialize(statement, out); err != nil {
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
