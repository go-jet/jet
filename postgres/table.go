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
	root ReadableTable
}

// Generates a select query on the current tableName.
func (r readableTableInterfaceImpl) SELECT(projection1 Projection, projections ...Projection) SelectStatement {
	return newSelectStatement(jet.SelectStatementType, r.root, append([]Projection{projection1}, projections...))
}

// Creates a inner join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) INNER_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.root, table, jet.InnerJoin, onCondition)
}

// Creates a left join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) LEFT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.root, table, jet.LeftJoin, onCondition)
}

// Creates a right join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.root, table, jet.RightJoin, onCondition)
}

func (r readableTableInterfaceImpl) FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.root, table, jet.FullJoin, onCondition)
}

func (r readableTableInterfaceImpl) CROSS_JOIN(table ReadableTable) ReadableTable {
	return newJoinTable(r.root, table, jet.CrossJoin, nil)
}

type writableTableInterfaceImpl struct {
	root WritableTable
}

func (w *writableTableInterfaceImpl) INSERT(columns ...jet.Column) InsertStatement {
	return newInsertStatement(w.root, jet.UnwidColumnList(columns))
}

func (w *writableTableInterfaceImpl) UPDATE(columns ...jet.Column) UpdateStatement {
	return newUpdateStatement(w.root, jet.UnwidColumnList(columns))
}

func (w *writableTableInterfaceImpl) DELETE() DeleteStatement {
	return newDeleteStatement(w.root)
}

func (w *writableTableInterfaceImpl) LOCK() LockStatement {
	return LOCK(w.root)
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

	t.readableTableInterfaceImpl.root = t
	t.writableTableInterfaceImpl.root = t

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

	newJoinTable.readableTableInterfaceImpl.root = newJoinTable

	return newJoinTable
}
