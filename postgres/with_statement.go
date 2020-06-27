package postgres

import "github.com/go-jet/jet/v2/internal/jet"

// CommonTableExpression contains information about a CTE.
type CommonTableExpression struct {
	readableTableInterfaceImpl
	jet.CommonTableExpression
}

// WITH function creates new WITH statement from list of common table expressions
func WITH(cte ...jet.CommonTableExpressionDefinition) func(statement jet.Statement) Statement {
	return jet.WITH(Dialect, cte...)
}

// CTE creates new named CommonTableExpression
func CTE(name string) CommonTableExpression {
	cte := CommonTableExpression{
		readableTableInterfaceImpl: readableTableInterfaceImpl{},
		CommonTableExpression:      jet.CTE(name),
	}

	cte.parent = &cte

	return cte
}
