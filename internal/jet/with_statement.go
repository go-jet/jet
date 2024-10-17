package jet

import "fmt"

// WITH function creates new with statement from list of common table expressions for specified dialect
func WITH(dialect Dialect, recursive bool, cte ...*CommonTableExpression) func(statement Statement) Statement {
	newWithImpl := &withImpl{
		recursive: recursive,
		ctes:      cte,
		serializerStatementInterfaceImpl: serializerStatementInterfaceImpl{
			dialect:       dialect,
			statementType: WithStatementType,
		},
	}
	newWithImpl.parent = newWithImpl

	return func(primaryStatement Statement) Statement {
		serializerStatement, ok := primaryStatement.(SerializerStatement)
		if !ok {
			panic("jet: unsupported main WITH statement.")
		}
		newWithImpl.primaryStatement = serializerStatement
		return newWithImpl
	}
}

type withImpl struct {
	serializerStatementInterfaceImpl
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

		cte.serialize(statement, out, FallTrough(options)...)
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
