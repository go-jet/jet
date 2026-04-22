package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// CommonTableExpression defines interface methods for CUBRID CTEs
type CommonTableExpression interface {
	SelectTable
	AS(statement jet.SerializerHasProjections) CommonTableExpression
	ALIAS(alias string) SelectTable
	internalCTE() *jet.CommonTableExpression
}

type commonTableExpression struct {
	readableTableInterfaceImpl
	jet.CommonTableExpression
}

// WITH creates new WITH statement
func WITH(cte ...CommonTableExpression) func(statement jet.Statement) Statement {
	return jet.WITH(Dialect, false, toInternalCTE(cte)...)
}

// WITH_RECURSIVE creates new WITH RECURSIVE statement
func WITH_RECURSIVE(cte ...CommonTableExpression) func(statement jet.Statement) Statement {
	return jet.WITH(Dialect, true, toInternalCTE(cte)...)
}

// CTE creates new named commonTableExpression
func CTE(name string, columns ...jet.ColumnExpression) CommonTableExpression {
	cte := &commonTableExpression{CommonTableExpression: jet.CTE(name, columns...)}
	cte.root = cte
	return cte
}

func (c *commonTableExpression) AS(stmt jet.SerializerHasProjections) CommonTableExpression {
	c.CommonTableExpression.Statement = stmt; return c
}
func (c *commonTableExpression) internalCTE() *jet.CommonTableExpression { return &c.CommonTableExpression }
func (c *commonTableExpression) ALIAS(name string) SelectTable           { return newSelectTable(c, name, nil) }

func toInternalCTE(ctes []CommonTableExpression) []*jet.CommonTableExpression {
	ret := make([]*jet.CommonTableExpression, len(ctes))
	for i, cte := range ctes {
		ret[i] = cte.internalCTE()
	}
	return ret
}
