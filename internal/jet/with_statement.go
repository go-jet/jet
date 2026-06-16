package jet

import "fmt"

// WITH function creates new with statement from list of common table expressions for specified dialect.
// The returned statement is a SerializerStatement (not just a Statement) so it can be used wherever a serializable
// statement is expected, for instance as the source of an INSERT ... QUERY(...).
func WITH(dialect Dialect, recursive bool, cte ...*CommonTableExpression) func(statement Statement) SerializerStatement {
	newWithImpl := &withImpl{
		recursive: recursive,
		ctes:      cte,
		statementInterfaceImpl: statementInterfaceImpl{
			dialect:       dialect,
			statementType: WithStatementType,
		},
	}
	newWithImpl.root = newWithImpl

	return func(primaryStatement Statement) SerializerStatement {
		serializerStatement, ok := primaryStatement.(SerializerStatement)
		if !ok {
			panic("jet: unsupported main WITH statement.")
		}
		newWithImpl.primaryStatement = serializerStatement
		return newWithImpl
	}
}

type withImpl struct {
	statementInterfaceImpl
	recursive        bool
	ctes             []*CommonTableExpression
	primaryStatement SerializerStatement
}

func (w withImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.NewLine()
	out.WriteString("WITH")

	if w.recursive {
		out.WriteString("RECURSIVE")
	}

	for i, cte := range w.ctes {
		if i > 0 {
			out.WriteString(",")
		}

		// A CTE in a WITH clause is always serialized as a definition (name AS (...)), regardless of the enclosing
		// statement type. Forwarding the incoming statement type instead would render the CTE in its FROM-clause form
		// (just the name) whenever the WITH is nested, for example as the source query of an INSERT ... QUERY(WITH(...)).
		cte.serialize(WithStatementType, out, FallTrough(options)...)
	}
	w.primaryStatement.serialize(statement, out, NoWrap.WithFallTrough(options)...)
}

func (w withImpl) projections() ProjectionList {
	return ProjectionList{}
}

// CommonTableExpression contains information about a CTE.
type CommonTableExpression struct {
	selectTableImpl

	NotMaterialized bool
	Columns         []ColumnExpression
}

// CTE creates new named CommonTableExpression
func CTE(name string, columns ...ColumnExpression) CommonTableExpression {
	cte := CommonTableExpression{
		selectTableImpl: NewSelectTable(nil, name, columns),
		Columns:         columns,
	}

	for _, column := range cte.Columns {
		column.setSubQuery(cte)
	}

	return cte
}

func (c CommonTableExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if statement == WithStatementType { // serialize CTE definition
		out.WriteIdentifier(c.alias)
		if len(c.Columns) > 0 {
			out.WriteByte('(')
			SerializeColumnExpressionNames(c.Columns, out)
			out.WriteByte(')')
		}
		out.WriteString("AS")

		if c.NotMaterialized {
			out.WriteString("NOT MATERIALIZED")
		}

		if c.Statement == nil {
			panic(fmt.Sprintf("jet: '%s' CTE is not defined", c.alias))
		}

		c.Statement.serialize(statement, out, FallTrough(options)...)

	} else { // serialize CTE in FROM clause
		out.WriteIdentifier(c.alias)
	}
}
