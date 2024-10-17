package postgres

import "github.com/go-jet/jet/v2/internal/jet"

// CommonTableExpression defines set of interface methods for postgres CTEs
type CommonTableExpression interface {
	SelectTable

	AS(statement jet.SerializerHasProjections) CommonTableExpression
	AS_NOT_MATERIALIZED(statement jet.SerializerStatement) CommonTableExpression
	// ALIAS is used to create another alias of the CTE, if a CTE needs to appear multiple times in the main query.
	ALIAS(alias string) SelectTable

	internalCTE() *jet.CommonTableExpression
}

type commonTableExpression struct {
	readableTableInterfaceImpl
	jet.CommonTableExpression
}

// WITH function creates new WITH statement from list of common table expressions
func WITH(cte ...CommonTableExpression) func(statement jet.Statement) Statement {
	return jet.WITH(Dialect, false, toInternalCTE(cte)...)
}

// WITH_RECURSIVE function creates new WITH RECURSIVE statement from list of common table expressions
func WITH_RECURSIVE(cte ...CommonTableExpression) func(statement jet.Statement) Statement {
	return jet.WITH(Dialect, true, toInternalCTE(cte)...)
}

// CTE creates new named commonTableExpression
func CTE(name string, columns ...jet.ColumnExpression) CommonTableExpression {
	cte := &commonTableExpression{
		readableTableInterfaceImpl: readableTableInterfaceImpl{},
		CommonTableExpression:      jet.CTE(name, columns...),
	}

	cte.parent = cte

	return cte
}

// AS is used to define a CTE query
func (c *commonTableExpression) AS(statement jet.SerializerHasProjections) CommonTableExpression {
	c.CommonTableExpression.Statement = statement
	return c
}

// AS_NOT_MATERIALIZED is used to define not materialized CTE query
func (c *commonTableExpression) AS_NOT_MATERIALIZED(statement jet.SerializerStatement) CommonTableExpression {
	c.CommonTableExpression.NotMaterialized = true
	c.CommonTableExpression.Statement = statement
	return c
}

func (c *commonTableExpression) internalCTE() *jet.CommonTableExpression {
	return &c.CommonTableExpression
}

// ALIAS is used to create another alias of the CTE, if a CTE needs to appear multiple times in the main query.
func (c *commonTableExpression) ALIAS(name string) SelectTable {
	return newSelectTable(c, name, nil)
}

func toInternalCTE(ctes []CommonTableExpression) []*jet.CommonTableExpression {
	var ret []*jet.CommonTableExpression

	for _, cte := range ctes {
		ret = append(ret, cte.internalCTE())
	}

	return ret
}
