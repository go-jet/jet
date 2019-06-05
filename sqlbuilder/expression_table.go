package sqlbuilder

import "errors"

type expressionTable interface {
	ReadableTable

	RefIntColumnName(name string) *IntegerColumn
	RefIntColumn(column Column) *IntegerColumn
	RefStringColumn(column Column) *StringColumn
}

type expressionTableImpl struct {
	readableTableInterfaceImpl
	expression Expression
	alias      string
}

func newExpressionTable(expression Expression, alias string) expressionTable {
	expTable := &expressionTableImpl{expression: expression, alias: alias}

	expTable.readableTableInterfaceImpl.parent = expTable

	return expTable
}

// Returns the tableName's name in the database
func (e *expressionTableImpl) SchemaName() string {
	return ""
}

func (e *expressionTableImpl) TableName() string {
	return e.alias
}

func (e *expressionTableImpl) RefIntColumnName(name string) *IntegerColumn {
	intColumn := NewIntegerColumn(name, false)
	intColumn.setTableName(e.alias)

	return intColumn
}

func (e *expressionTableImpl) RefIntColumn(column Column) *IntegerColumn {
	intColumn := NewIntegerColumn(column.TableName()+"."+column.Name(), false)
	intColumn.setTableName(e.alias)

	return intColumn
}

func (e *expressionTableImpl) RefStringColumn(column Column) *StringColumn {
	strColumn := NewStringColumn(column.TableName()+"."+column.Name(), false)
	strColumn.setTableName(e.alias)
	return strColumn
}

func (e *expressionTableImpl) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	if e == nil {
		return errors.New("Expression table is nil. ")
	}
	//out.writeString("( ")
	err := e.expression.serialize(statement, out)

	if err != nil {
		return err
	}

	out.writeString("AS")
	out.writeString(e.alias)

	return nil
}
