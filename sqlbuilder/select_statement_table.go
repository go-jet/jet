package sqlbuilder

import "bytes"

type SelectStatementTable struct {
	statement SelectStatement
	columns   []Column
	alias     string
}

func (s *SelectStatementTable) Columns() []Column {
	return s.columns
}

func (s *SelectStatementTable) Column(name string) Column {
	return &baseColumn{
		name:      name,
		tableName: s.alias,
	}
}

func (s *SelectStatementTable) ColumnFrom(column Column) Column {
	return &baseColumn{
		name:      column.TableName() + "." + column.Name(),
		tableName: s.alias,
	}
}

func (s *SelectStatementTable) SerializeSql(out *bytes.Buffer) error {
	out.WriteString("( ")
	statementStr, err := s.statement.String()

	if err != nil {
		return err
	}

	out.WriteString(statementStr)

	out.WriteString(" ) AS ")
	out.WriteString(s.alias)

	return nil
}

// Generates a select query on the current tableName.
func (s *SelectStatementTable) Select(projections ...Expression) SelectStatement {
	return newSelectStatement(s, projections)
}

// Creates a inner join tableName expression using onCondition.
func (s *SelectStatementTable) InnerJoinOn(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return InnerJoinOn(s, table, onCondition)
}

//func (s *SelectStatementTable) InnerJoinUsing(table ReadableTable, col1 Column, col2 Column) ReadableTable {
//	return InnerJoinOn(s, table, col1.Eq(col2))
//}

// Creates a left join tableName expression using onCondition.
func (s *SelectStatementTable) LeftJoinOn(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return LeftJoinOn(s, table, onCondition)
}

// Creates a right join tableName expression using onCondition.
func (s *SelectStatementTable) RightJoinOn(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return RightJoinOn(s, table, onCondition)
}

func (s *SelectStatementTable) FullJoin(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return FullJoin(s, table, onCondition)
}

func (s *SelectStatementTable) CrossJoin(table ReadableTable) ReadableTable {
	return CrossJoin(s, table)
}
