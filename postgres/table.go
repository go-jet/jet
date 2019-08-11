package postgres

import "github.com/go-jet/jet/internal/jet"

type readableTable interface {
	// Generates a select query on the current tableName.
	SELECT(projection jet.Projection, projections ...jet.Projection) SelectStatement

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
	INSERT(columns ...jet.IColumn) InsertStatement
	UPDATE(column jet.IColumn, columns ...jet.IColumn) UpdateStatement
	DELETE() DeleteStatement
	LOCK() LockStatement
}

// ReadableTable interface
type ReadableTable interface {
	//table
	readableTable
	jet.Serializer
	//acceptsVisitor
}

type WritableTable interface {
	jet.TableInterface
	writableTable
	jet.Serializer
}

type Table interface {
	//table
	readableTable
	writableTable
	jet.Serializer
	//acceptsVisitor

	SchemaName() string
	TableName() string
	AS(alias string)
}

type readableTableInterfaceImpl struct {
	parent ReadableTable
}

// Generates a select query on the current tableName.
func (r *readableTableInterfaceImpl) SELECT(projection1 jet.Projection, projections ...jet.Projection) SelectStatement {
	return newSelectStatement(r.parent, append([]jet.Projection{projection1}, projections...))
}

// Creates a inner join tableName Expression using onCondition.
func (r *readableTableInterfaceImpl) INNER_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.InnerJoin, onCondition)
}

// Creates a left join tableName Expression using onCondition.
func (r *readableTableInterfaceImpl) LEFT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.LeftJoin, onCondition)
}

// Creates a right join tableName Expression using onCondition.
func (r *readableTableInterfaceImpl) RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.RightJoin, onCondition)
}

func (r *readableTableInterfaceImpl) FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, jet.FullJoin, onCondition)
}

func (r *readableTableInterfaceImpl) CROSS_JOIN(table ReadableTable) ReadableTable {
	return newJoinTable(r.parent, table, jet.CrossJoin, nil)
}

type writableTableInterfaceImpl struct {
	parent WritableTable
}

func (w *writableTableInterfaceImpl) INSERT(columns ...jet.IColumn) InsertStatement {
	return newInsertStatement(w.parent, jet.UnwidColumnList(columns))
}

func (w *writableTableInterfaceImpl) UPDATE(column jet.IColumn, columns ...jet.IColumn) UpdateStatement {
	return newUpdateStatement(w.parent, jet.UnwindColumns(column, columns...))
}

func (w *writableTableInterfaceImpl) DELETE() DeleteStatement {
	return newDeleteStatement(w.parent)
}

func (w *writableTableInterfaceImpl) LOCK() LockStatement {
	return LOCK(w.parent)
}

type table2Impl struct {
	readableTableInterfaceImpl
	writableTableInterfaceImpl

	jet.TableImpl2
}

func NewTable(schemaName, name string, columns ...jet.Column) Table {

	t := &table2Impl{
		TableImpl2: jet.NewTable2(Dialect, schemaName, name, columns...),
	}

	t.readableTableInterfaceImpl.parent = t
	t.writableTableInterfaceImpl.parent = t

	return t
}

type joinTable2 struct {
	readableTableInterfaceImpl
	jet.JoinTableImpl
}

func newJoinTable(lhs jet.Serializer, rhs jet.Serializer, joinType jet.JoinType, onCondition BoolExpression) ReadableTable {
	newJoinTable := &joinTable2{
		JoinTableImpl: jet.NewJoinTableImpl(lhs, rhs, joinType, onCondition),
	}

	newJoinTable.readableTableInterfaceImpl.parent = newJoinTable

	return newJoinTable
}
