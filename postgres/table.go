package postgres

import "github.com/go-jet/jet/v2/internal/jet"

// Table is interface for MySQL tables
type Table interface {
	readableTable
	writableTable
	jet.SerializerTable
}

type readableTable interface {
	// Generates a select query on the current tableName.
	SELECT(projection Projection, projections ...Projection) SelectStatement

	// Creates a inner join tableName Expression using onCondition.
	INNER_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a left join tableName Expression using onCondition.
	LEFT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a right join tableName Expression using onCondition.
	RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a full join tableName Expression using onCondition.
	FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a cross join tableName Expression using onCondition.
	CROSS_JOIN(table ReadableTable) ReadableTable
}

type writableTable interface {
	INSERT(columns ...jet.Column) InsertStatement
	UPDATE(columns ...jet.Column) UpdateStatement
	DELETE() DeleteStatement
	LOCK() LockStatement
}

// ReadableTable interface
type ReadableTable interface {
	readableTable
	jet.Serializer
}

// WritableTable interface
type WritableTable interface {
	jet.Table
	writableTable
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
func (r readableTableInterfaceImpl) INNER_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.InnerJoin, onCondition)
}

// Creates a left join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) LEFT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.LeftJoin, onCondition)
}

// Creates a right join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.RightJoin, onCondition)
}

func (r readableTableInterfaceImpl) FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.FullJoin, onCondition)
}

func (r readableTableInterfaceImpl) CROSS_JOIN(table ReadableTable) ReadableTable {
	return newJoinTable(r.parent, table, jet.CrossJoin, nil)
}

type writableTableInterfaceImpl struct {
	parent WritableTable
}

func (w *writableTableInterfaceImpl) INSERT(columns ...jet.Column) InsertStatement {
	return newInsertStatement(w.parent, jet.UnwidColumnList(columns))
}

func (w *writableTableInterfaceImpl) UPDATE(columns ...jet.Column) UpdateStatement {
	return newUpdateStatement(w.parent, jet.UnwidColumnList(columns))
}

func (w *writableTableInterfaceImpl) DELETE() DeleteStatement {
	return newDeleteStatement(w.parent)
}

func (w *writableTableInterfaceImpl) LOCK() LockStatement {
	return LOCK(w.parent)
}

type tableImpl struct {
	readableTableInterfaceImpl
	writableTableInterfaceImpl

	jet.SerializerTable
}

// NewTable creates new table with schema Name, table Name and list of columns
func NewTable(schemaName, name, alias string, columns ...jet.ColumnExpression) Table {

	t := &tableImpl{
		SerializerTable: jet.NewTable(schemaName, name, alias, columns...),
	}

	t.readableTableInterfaceImpl.parent = t
	t.writableTableInterfaceImpl.parent = t

	return t
}

type joinTable struct {
	readableTableInterfaceImpl
	jet.JoinTable
}

func newJoinTable(lhs jet.Serializer, rhs jet.Serializer, joinType jet.JoinType, onCondition BoolExpression) ReadableTable {
	newJoinTable := &joinTable{
		JoinTable: jet.NewJoinTable(lhs, rhs, joinType, onCondition),
	}

	newJoinTable.readableTableInterfaceImpl.parent = newJoinTable

	return newJoinTable
}
