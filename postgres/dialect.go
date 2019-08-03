package postgres

import (
	"github.com/go-jet/jet/internal/jet"
	"strconv"
)

var Dialect = NewDialect()

func NewDialect() jet.Dialect {

	dialectParams := jet.DialectParams{
		Name:                "PostgreSQL",
		PackageName:         "postgres",
		CastOverride:        castFunc,
		AliasQuoteChar:      '"',
		IdentifierQuoteChar: '"',
		ArgumentPlaceholder: func(ord int) string {
			return "$" + strconv.Itoa(ord)
		},
		UpdateAssigment:   postgresUpdateAssigment,
		SupportsReturning: true,
	}

	return jet.NewDialect(dialectParams)
}

func castFunc(expression jet.Expression, castType string) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) error {
		if err := jet.Serialize(expression, statement, out, options...); err != nil {
			return err
		}
		out.WriteString("::" + castType)
		return nil
	}
}

func postgresUpdateAssigment(columns []jet.IColumn, values []jet.Clause, out *jet.SqlBuilder) (err error) {
	if len(columns) > 1 {
		out.WriteString("(")
	}

	err = jet.SerializeColumnNames(columns, out)

	if err != nil {
		return
	}

	if len(columns) > 1 {
		out.WriteString(")")
	}

	out.WriteString("=")

	if len(values) > 1 {
		out.WriteString("(")
	}

	err = jet.SerializeClauseList(jet.UpdateStatementType, values, out)

	if err != nil {
		return
	}

	if len(values) > 1 {
		out.WriteString(")")
	}

	return
}
