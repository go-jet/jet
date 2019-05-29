package sqlbuilder

import "errors"

type expressionTable interface {
	readableTable

	RefIntColumnName(name string) *IntegerColumn
	RefIntColumn(column column) *IntegerColumn
	RefStringColumn(column column) *StringColumn
}

type expressionTableImpl struct {
	statement expression
	alias     string
}

// Returns the tableName's name in the database
func (e *expressionTableImpl) SchemaName() string {
	return ""
}

func (e *expressionTableImpl) TableName() string {
	return e.alias
}

func (e *expressionTableImpl) Columns() []column {
	return []column{}
}

func (e *expressionTableImpl) RefIntColumnName(name string) *IntegerColumn {
	intColumn := NewIntegerColumn(name, NotNullable)
	intColumn.setTableName(e.alias)

	return intColumn
}

func (e *expressionTableImpl) RefIntColumn(column column) *IntegerColumn {
	intColumn := NewIntegerColumn(column.TableName()+"."+column.Name(), NotNullable)
	intColumn.setTableName(e.alias)

	return intColumn
}

func (e *expressionTableImpl) RefStringColumn(column column) *StringColumn {
	strColumn := NewStringColumn(column.TableName()+"."+column.Name(), NotNullable)
	strColumn.setTableName(e.alias)
	return strColumn
}

func (e *expressionTableImpl) serialize(statement statementType, out *queryData) error {
	if e == nil {
		return errors.New("Expression table is nil. ")
	}
	//out.writeString("( ")
	err := e.statement.serialize(statement, out)

	if err != nil {
		return err
	}

	out.writeString("AS")
	out.writeString(e.alias)

	return nil
}

// Generates a select query on the current tableName.
func (e *expressionTableImpl) SELECT(projections ...projection) selectStatement {
	return newSelectStatement(e, projections)
}

// Creates a inner join tableName expression using onCondition.
func (e *expressionTableImpl) INNER_JOIN(table readableTable, onCondition boolExpression) readableTable {
	return InnerJoinOn(e, table, onCondition)
}

//func (s *expressionTableImpl) InnerJoinUsing(table readableTable, col1 column, col2 column) readableTable {
//	return INNER_JOIN(s, table, col1.EQ(col2))
//}

// Creates a left join tableName expression using onCondition.
func (e *expressionTableImpl) LEFT_JOIN(table readableTable, onCondition boolExpression) readableTable {
	return LeftJoinOn(e, table, onCondition)
}

// Creates a right join tableName expression using onCondition.
func (e *expressionTableImpl) RIGHT_JOIN(table readableTable, onCondition boolExpression) readableTable {
	return RightJoinOn(e, table, onCondition)
}

func (e *expressionTableImpl) FULL_JOIN(table readableTable, onCondition boolExpression) readableTable {
	return FullJoin(e, table, onCondition)
}

func (e *expressionTableImpl) CROSS_JOIN(table readableTable) readableTable {
	return CrossJoin(e, table)
}
