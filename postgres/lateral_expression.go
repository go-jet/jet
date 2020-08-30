package postgres

import (
	"github.com/go-jet/jet/v2/internal/jet"
)

type LateralExpression interface {
	As(alias string) LateralTable
}

type LateralTable interface {
	jet.LateralTable
	readableTable
}

type lateralImpl struct {
	selectStmt SelectStatement
}

type lateralTableImpl struct {
	jet.LateralTable
	readableTable
}

func newLateralExpression(selectStmt SelectStatement) LateralExpression {
	return &lateralImpl{
		selectStmt: selectStmt,
	}
}

// NewLateral func
func LATERAL(selectStmt SelectStatement) LateralExpression {
	return newLateralExpression(selectStmt)
}

func (l *lateralImpl) As(alias string) LateralTable {
	subQuery := &lateralTableImpl{
		LateralTable: jet.NewLateralTable(l.selectStmt, alias),
	}
	return subQuery
}
