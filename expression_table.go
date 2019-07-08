package jet

import "errors"

type ExpressionTable interface {
	ReadableTable

	Alias() string

	AllColumns() ProjectionList
}

type expressionTableImpl struct {
	readableTableInterfaceImpl
	expression Expression
	alias      string

	projections []projection
}

func newExpressionTable(expression Expression, alias string, projections []projection) ExpressionTable {
	expTable := &expressionTableImpl{expression: expression, alias: alias}

	expTable.readableTableInterfaceImpl.parent = expTable

	for _, projection := range projections {
		newProjection := projection.from(expTable)

		expTable.projections = append(expTable.projections, newProjection)
	}

	return expTable
}

func (e *expressionTableImpl) Alias() string {
	return e.alias
}

func (e *expressionTableImpl) columns() []column {
	return nil
}

func (e *expressionTableImpl) AllColumns() ProjectionList {
	return e.projections
}

func (e *expressionTableImpl) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	if e == nil {
		return errors.New("Expression table is nil. ")
	}

	err := e.expression.serialize(statement, out)

	if err != nil {
		return err
	}

	out.writeString("AS")
	out.writeIdentifier(e.alias)

	return nil
}
