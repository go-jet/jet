package sqlbuilder

type ExpressionTable interface {
	ReadableTable

	RefIntColumnName(name string) *IntegerColumn
	RefIntColumn(column Column) *IntegerColumn
	RefStringColumn(column Column) *StringColumn
}

type expressionTableImpl struct {
	statement Expression
	columns   []Column
	alias     string
}

// Returns the tableName's name in the database
func (t *expressionTableImpl) SchemaName() string {
	return ""
}

func (s *expressionTableImpl) TableName() string {
	return s.alias
}

func (s *expressionTableImpl) Columns() []Column {
	return s.columns
}

func (s *expressionTableImpl) RefIntColumnName(name string) *IntegerColumn {
	intColumn := NewIntegerColumn(name, NotNullable)
	intColumn.setTableName(s.alias)

	return intColumn
}

func (s *expressionTableImpl) RefIntColumn(column Column) *IntegerColumn {
	intColumn := NewIntegerColumn(column.TableName()+"."+column.Name(), NotNullable)
	intColumn.setTableName(s.alias)

	return intColumn
}

func (s *expressionTableImpl) RefStringColumn(column Column) *StringColumn {
	strColumn := NewStringColumn(column.Name(), NotNullable)
	strColumn.setTableName(column.TableName())
	return strColumn
}

func (s *expressionTableImpl) SerializeSql(out *queryData) error {
	out.WriteString("( ")
	err := s.statement.Serialize(out)

	if err != nil {
		return err
	}

	out.WriteString(" ) AS ")
	out.WriteString(s.alias)

	return nil
}

// Generates a select query on the current tableName.
func (s *expressionTableImpl) SELECT(projections ...Projection) SelectStatement {
	return newSelectStatement(s, projections)
}

// Creates a inner join tableName expression using onCondition.
func (s *expressionTableImpl) INNER_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return InnerJoinOn(s, table, onCondition)
}

//func (s *expressionTableImpl) InnerJoinUsing(table ReadableTable, col1 Column, col2 Column) ReadableTable {
//	return INNER_JOIN(s, table, col1.Eq(col2))
//}

// Creates a left join tableName expression using onCondition.
func (s *expressionTableImpl) LEFT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return LeftJoinOn(s, table, onCondition)
}

// Creates a right join tableName expression using onCondition.
func (s *expressionTableImpl) RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return RightJoinOn(s, table, onCondition)
}

func (s *expressionTableImpl) FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return FullJoin(s, table, onCondition)
}

func (s *expressionTableImpl) CROSS_JOIN(table ReadableTable) ReadableTable {
	return CrossJoin(s, table)
}
