package mysql

import "github.com/go-jet/jet/v2/internal/jet"

// Table is interface for MySQL tables
type Table interface {
	jet.SerializerTable
	readableTable

	INSERT(columns ...jet.Column) InsertStatement
	UPDATE(columns ...jet.Column) UpdateStatement
	DELETE() DeleteStatement
	LOCK() LockStatement
}

type readableTable interface {
	// Generates a select query on the current tableName.
	SELECT(projection Projection, projections ...Projection) SelectStatement

	// Creates a inner join tableName Expression using onCondition.
	INNER_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable

	// Creates a left join tableName Expression using onCondition.
	LEFT_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable

	// Creates a right join tableName Expression using onCondition.
	RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable

	// Creates a full join tableName Expression using onCondition.
	FULL_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable

	// Creates a cross join tableName Expression using onCondition.
	CROSS_JOIN(table ReadableTable) joinSelectUpdateTable
}

type joinSelectUpdateTable interface {
	ReadableTable
	UPDATE(columns ...jet.Column) UpdateStatement
}

// ReadableTable interface
type ReadableTable interface {
	readableTable
	jet.Serializer
}

type readableTableInterfaceImpl struct {
	parent ReadableTable
}

// Generates a select query on the current tableName.
func (r readableTableInterfaceImpl) SELECT(projection1 Projection, projections ...Projection) SelectStatement {
	return newSelectStatement(r.parent, append([]Projection{projection1}, projections...))
}

// Creates a inner join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) INNER_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable {
	return newJoinTable(r.parent, table, jet.InnerJoin, onCondition)
}

// Creates a left join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) LEFT_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable {
	return newJoinTable(r.parent, table, jet.LeftJoin, onCondition)
}

// Creates a right join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable {
	return newJoinTable(r.parent, table, jet.RightJoin, onCondition)
}

func (r readableTableInterfaceImpl) FULL_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable {
	return newJoinTable(r.parent, table, jet.FullJoin, onCondition)
}

func (r readableTableInterfaceImpl) CROSS_JOIN(table ReadableTable) joinSelectUpdateTable {
	return newJoinTable(r.parent, table, jet.CrossJoin, nil)
}

// NewTable creates new table with schema Name, table Name and list of columns
func NewTable(schemaName, name, alias string, columns ...jet.ColumnExpression) Table {
	t := &tableImpl{
		SerializerTable: jet.NewTable(schemaName, name, alias, columns...),
	}

	t.readableTableInterfaceImpl.parent = t
	t.parent = t

	return t
}

type tableImpl struct {
	jet.SerializerTable
	readableTableInterfaceImpl
	parent Table
}

func (t *tableImpl) INSERT(columns ...jet.Column) InsertStatement {
	return newInsertStatement(t.parent, jet.UnwidColumnList(columns))
}

func (t *tableImpl) UPDATE(columns ...jet.Column) UpdateStatement {
	return newUpdateStatement(t.parent, jet.UnwidColumnList(columns))
}

func (t *tableImpl) DELETE() DeleteStatement {
	return newDeleteStatement(t.parent)
}

func (t *tableImpl) LOCK() LockStatement {
	return LOCK(t.parent)
}

type joinTable struct {
	tableImpl
	jet.JoinTable
}

func newJoinTable(lhs jet.Serializer, rhs jet.Serializer, joinType jet.JoinType, onCondition BoolExpression) Table {
	newJoinTable := &joinTable{
		JoinTable: jet.NewJoinTable(lhs, rhs, joinType, onCondition),
	}

	newJoinTable.readableTableInterfaceImpl.parent = newJoinTable
	newJoinTable.parent = newJoinTable

	return newJoinTable
}
