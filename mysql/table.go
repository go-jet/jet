package mysql

import "github.com/go-jet/jet/internal/jet"

//type Table jet.Table
//
//func NewTable(schemaName, name string, columns ...jet.Column) Table {
//	return jet.NewTable(Dialect, schemaName, name, columns...)
//}

type Table interface {
	jet.SerializerTable
	readableTable

	INSERT(columns ...jet.IColumn) InsertStatement
	UPDATE(column jet.IColumn, columns ...jet.IColumn) UpdateStatement
	DELETE() DeleteStatement
	//LOCK() LockStatement

	AS(alias string)
}

type readableTable interface {
	// Generates a select query on the current tableName.
	SELECT(projection jet.Projection, projections ...jet.Projection) SelectStatement

	// Creates a inner join tableName Expression using onCondition.
	INNER_JOIN(table ReadableTable, onCondition BoolExpression) Table

	// Creates a left join tableName Expression using onCondition.
	LEFT_JOIN(table ReadableTable, onCondition BoolExpression) Table

	// Creates a right join tableName Expression using onCondition.
	RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) Table

	// Creates a full join tableName Expression using onCondition.
	FULL_JOIN(table ReadableTable, onCondition BoolExpression) Table

	// Creates a cross join tableName Expression using onCondition.
	CROSS_JOIN(table ReadableTable) Table
}

type ReadableTable interface {
	jet.SerializerTable
	readableTable
}

type readableTableInterfaceImpl struct {
	parent ReadableTable
}

// Generates a select query on the current tableName.
func (r *readableTableInterfaceImpl) SELECT(projection1 jet.Projection, projections ...jet.Projection) SelectStatement {
	return newSelectStatement(r.parent, append([]jet.Projection{projection1}, projections...))
}

// Creates a inner join tableName Expression using onCondition.
func (r *readableTableInterfaceImpl) INNER_JOIN(table ReadableTable, onCondition BoolExpression) Table {
	return newJoinTable(r.parent, table, jet.InnerJoin, onCondition)
}

// Creates a left join tableName Expression using onCondition.
func (r *readableTableInterfaceImpl) LEFT_JOIN(table ReadableTable, onCondition BoolExpression) Table {
	return newJoinTable(r.parent, table, jet.LeftJoin, onCondition)
}

// Creates a right join tableName Expression using onCondition.
func (r *readableTableInterfaceImpl) RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) Table {
	return newJoinTable(r.parent, table, jet.RightJoin, onCondition)
}

func (r *readableTableInterfaceImpl) FULL_JOIN(table ReadableTable, onCondition BoolExpression) Table {
	return newJoinTable(r.parent, table, jet.FullJoin, onCondition)
}

func (r *readableTableInterfaceImpl) CROSS_JOIN(table ReadableTable) Table {
	return newJoinTable(r.parent, table, jet.CrossJoin, nil)
}

func NewTable(schemaName, name string, columns ...jet.Column) Table {
	t := &tableImpl{
		TableImpl2: jet.NewTable2(Dialect, schemaName, name, columns...),
	}

	t.readableTableInterfaceImpl.parent = t
	t.parent = t

	return t
}

type tableImpl struct {
	jet.TableImpl2
	readableTableInterfaceImpl
	parent Table
}

func (w *tableImpl) INSERT(columns ...jet.IColumn) InsertStatement {
	return newInsertStatement(w.parent, jet.UnwidColumnList(columns))
}

func (w *tableImpl) UPDATE(column jet.IColumn, columns ...jet.IColumn) UpdateStatement {
	return newUpdateStatement(w.parent, jet.UnwindColumns(column, columns...))
}

func (w *tableImpl) DELETE() DeleteStatement {
	return newDeleteStatement(w.parent)
}

//func (w *tableInterfaceImpl) LOCK() LockStatement {
//	return LOCK(w.parent)
//}

type joinTable2 struct {
	tableImpl
	jet.JoinTableImpl
}

func newJoinTable(lhs jet.Serializer, rhs jet.Serializer, joinType jet.JoinType, onCondition BoolExpression) Table {
	newJoinTable := &joinTable2{
		JoinTableImpl: jet.NewJoinTableImpl(lhs, rhs, joinType, onCondition),
	}

	newJoinTable.readableTableInterfaceImpl.parent = newJoinTable
	newJoinTable.parent = newJoinTable

	return newJoinTable
}
