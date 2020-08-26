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

	// Creates a Lateral query on the current tableName.
	LATERAL(table SelectTable) ReadableTable

	// Creates a inner join tableName Expression using onCondition.
	INNER_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a inner join lateral tableName Expression.
	INNER_JOIN_LATERAL(table SelectTable) ReadableTable

	// Creates a left join tableName Expression using onCondition.
	LEFT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a left join lateral tableName Expression.
	LEFT_JOIN_LATERAL(table SelectTable) ReadableTable

	// Creates a right join tableName Expression using onCondition.
	RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a right join lateral tableName Expression.
	RIGHT_JOIN_LATERAL(table SelectTable) ReadableTable

	// Creates a full join tableName Expression using onCondition.
	FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a full join lateral tableName Expression.
	FULL_JOIN_LATERAL(table SelectTable) ReadableTable

	// Creates a cross join tableName Expression using onCondition.
	CROSS_JOIN(table ReadableTable) ReadableTable

	// Creates a cross join lateral tableName Expression.
	CROSS_JOIN_LATERAL(table SelectTable) ReadableTable
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
func (r readableTableInterfaceImpl) LATERAL(table SelectTable) ReadableTable {
	return newLateralTable(r.parent, table)
}

// Creates a inner join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) INNER_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.InnerJoin, onCondition)
}

// Creates a inner join lateral tableName Expression.
func (r readableTableInterfaceImpl) INNER_JOIN_LATERAL(table SelectTable) ReadableTable {
	return newJoinTable(r.parent, table, jet.InnerJoinLateral, nil)
}

// Creates a left join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) LEFT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.LeftJoin, onCondition)
}

// Creates a left join lateral tableName Expression.
func (r readableTableInterfaceImpl) LEFT_JOIN_LATERAL(table SelectTable) ReadableTable {
	return newJoinTable(r.parent, table, jet.LeftJoinLateral, nil)
}

// Creates a right join tableName Expression using onCondition.
func (r readableTableInterfaceImpl) RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.RightJoin, onCondition)
}

// Creates a right join lateral tableName Expression.
func (r readableTableInterfaceImpl) RIGHT_JOIN_LATERAL(table SelectTable) ReadableTable {
	return newJoinTable(r.parent, table, jet.RightJoinLateral, nil)
}

func (r readableTableInterfaceImpl) FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.FullJoin, onCondition)
}

// Creates a full join lateral tableName Expression.
func (r readableTableInterfaceImpl) FULL_JOIN_LATERAL(table SelectTable) ReadableTable {
	return newJoinTable(r.parent, table, jet.FullJoinLateral, nil)
}

func (r readableTableInterfaceImpl) CROSS_JOIN(table ReadableTable) ReadableTable {
	return newJoinTable(r.parent, table, jet.CrossJoin, nil)
}

// Creates a cross join lateral tableName Expression.
func (r readableTableInterfaceImpl) CROSS_JOIN_LATERAL(table SelectTable) ReadableTable {
	return newJoinTable(r.parent, table, jet.CrossJoinLateral, nil)
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
func NewTable(schemaName, name string, columns ...jet.ColumnExpression) Table {

	t := &tableImpl{
		SerializerTable: jet.NewTable(schemaName, name, columns...),
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

type lateralTable struct {
	readableTableInterfaceImpl
	jet.LateralTable
}

func newLateralTable(lhs jet.Serializer, rhs jet.Serializer) ReadableTable {
	newLateralTable := &lateralTable{
		LateralTable: jet.NewLateralTable(lhs, rhs),
	}

	newLateralTable.readableTableInterfaceImpl.parent = newLateralTable

	return newLateralTable
}
