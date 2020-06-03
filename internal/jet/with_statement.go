package jet

// WITH function creates new with statement from list of common table expressions for specified dialect
func WITH(dialect Dialect, cte ...CommonTableExpressionDefinition) func(statement Statement) Statement {
	newWithImpl := &withImpl{
		ctes: cte,
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
	ctes             []CommonTableExpressionDefinition
	primaryStatement SerializerStatement
}

func (w withImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.NewLine()
	out.WriteString("WITH")

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
}

// CTE creates new named CommonTableExpression
func CTE(name string) CommonTableExpression {
	return CommonTableExpression{
		selectTableImpl: selectTableImpl{
			selectStmt: nil,
			alias:      name,
		},
	}
}

func (c CommonTableExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteIdentifier(c.alias)
}

// AS returns sets definition for a CTE
func (c *CommonTableExpression) AS(statement SerializerStatement) CommonTableExpressionDefinition {
	c.selectStmt = statement
	return CommonTableExpressionDefinition{cte: c}
}

// CommonTableExpressionDefinition contains implementation details of CTE
type CommonTableExpressionDefinition struct {
	cte *CommonTableExpression
}

func (c CommonTableExpressionDefinition) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteIdentifier(c.cte.alias)
	out.WriteString("AS")
	c.cte.selectStmt.serialize(statement, out, FallTrough(options)...)
}
