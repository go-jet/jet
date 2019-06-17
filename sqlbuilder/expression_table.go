package sqlbuilder

import "errors"

type ExpressionTable interface {
	ReadableTable

	Alias() string
}

type expressionTableImpl struct {
	readableTableInterfaceImpl
	expression Expression
	alias      string
}

func newExpressionTable(expression Expression, alias string) ExpressionTable {
	expTable := &expressionTableImpl{expression: expression, alias: alias}

	expTable.readableTableInterfaceImpl.parent = expTable

	return expTable
}

func (e *expressionTableImpl) Alias() string {
	return e.alias
}

func (e *expressionTableImpl) serialize(statement statementType, out *queryData, options ...serializeOption) error {
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
