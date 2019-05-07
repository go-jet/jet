package sqlbuilder

type expressionTable interface {
	readableTable

	RefIntColumnName(name string) *IntegerColumn
	RefIntColumn(column column) *IntegerColumn
	RefStringColumn(column column) *StringColumn
}

type expressionTableImpl struct {
	statement expression
	columns   []column
	alias     string
}

// Returns the tableName's name in the database
func (t *expressionTableImpl) SchemaName() string {
	return ""
}

func (s *expressionTableImpl) TableName() string {
	return s.alias
}

func (s *expressionTableImpl) Columns() []column {
	return s.columns
}

func (s *expressionTableImpl) RefIntColumnName(name string) *IntegerColumn {
	intColumn := NewIntegerColumn(name, NotNullable)
	intColumn.setTableName(s.alias)

	return intColumn
}

func (s *expressionTableImpl) RefIntColumn(column column) *IntegerColumn {
	intColumn := NewIntegerColumn(column.TableName()+"."+column.Name(), NotNullable)
	intColumn.setTableName(s.alias)

	return intColumn
}

func (s *expressionTableImpl) RefStringColumn(column column) *StringColumn {
	strColumn := NewStringColumn(column.Name(), NotNullable)
	strColumn.setTableName(column.TableName())
	return strColumn
}

func (s *expressionTableImpl) serializeSql(out *queryData) error {
	out.WriteString("( ")
	err := s.statement.serialize(out)

	if err != nil {
		return err
	}

	out.WriteString(" ) AS ")
	out.WriteString(s.alias)

	return nil
}

// Generates a select query on the current tableName.
func (s *expressionTableImpl) SELECT(projections ...projection) selectStatement {
	return newSelectStatement(s, projections)
}

// Creates a inner join tableName expression using onCondition.
func (s *expressionTableImpl) INNER_JOIN(table readableTable, onCondition boolExpression) readableTable {
	return InnerJoinOn(s, table, onCondition)
}

//func (s *expressionTableImpl) InnerJoinUsing(table readableTable, col1 column, col2 column) readableTable {
//	return INNER_JOIN(s, table, col1.Eq(col2))
//}

// Creates a left join tableName expression using onCondition.
func (s *expressionTableImpl) LEFT_JOIN(table readableTable, onCondition boolExpression) readableTable {
	return LeftJoinOn(s, table, onCondition)
}

// Creates a right join tableName expression using onCondition.
func (s *expressionTableImpl) RIGHT_JOIN(table readableTable, onCondition boolExpression) readableTable {
	return RightJoinOn(s, table, onCondition)
}

func (s *expressionTableImpl) FULL_JOIN(table readableTable, onCondition boolExpression) readableTable {
	return FullJoin(s, table, onCondition)
}

func (s *expressionTableImpl) CROSS_JOIN(table readableTable) readableTable {
	return CrossJoin(s, table)
}
