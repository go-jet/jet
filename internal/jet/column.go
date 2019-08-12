// Modeling of columns

package jet

type Column interface {
	Name() string
	TableName() string

	SetTableName(table string)
	SetSubQuery(subQuery SelectTable)
	DefaultAlias() string
}

// Column is common column interface for all types of columns.
type ColumnExpression interface {
	Column
	Expression
}

// The base type for real materialized columns.
type columnImpl struct {
	ExpressionInterfaceImpl

	name      string
	tableName string

	subQuery SelectTable
}

func newColumn(name string, tableName string, parent ColumnExpression) columnImpl {
	bc := columnImpl{
		name:      name,
		tableName: tableName,
	}

	bc.ExpressionInterfaceImpl.Parent = parent

	return bc
}

func (c *columnImpl) Name() string {
	return c.name
}

func (c *columnImpl) TableName() string {
	return c.tableName
}

func (c *columnImpl) SetTableName(table string) {
	c.tableName = table
}

func (c *columnImpl) SetSubQuery(subQuery SelectTable) {
	c.subQuery = subQuery
}

func (c *columnImpl) DefaultAlias() string {
	if c.tableName != "" {
		return c.tableName + "." + c.name
	}

	return c.name
}

func (c *columnImpl) serializeForOrderBy(statement StatementType, out *SqlBuilder) error {
	if statement == SetStatementType {
		// set Statement (UNION, EXCEPT ...) can reference only select projections in order by clause
		out.WriteAlias(c.DefaultAlias()) //always quote

		return nil
	}

	return c.serialize(statement, out)
}

func (c columnImpl) serializeForProjection(statement StatementType, out *SqlBuilder) error {
	err := c.serialize(statement, out)

	if err != nil {
		return err
	}

	out.WriteString("AS")
	out.WriteAlias(c.DefaultAlias())

	return nil
}

func (c columnImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {

	if c.subQuery != nil {
		out.WriteIdentifier(c.subQuery.Alias())
		out.WriteByte('.')
		out.WriteIdentifier(c.DefaultAlias(), true)
	} else {
		if c.tableName != "" {
			out.WriteIdentifier(c.tableName)
			out.WriteByte('.')
		}

		out.WriteIdentifier(c.name)
	}

	return nil
}

//------------------------------------------------------//

type IColumnList interface {
	Projection
	Column

	Columns() []ColumnExpression
}

func ColumnList(columns ...ColumnExpression) IColumnList {
	return columnListImpl(columns)
}

// ColumnList is redefined type to support list of columns as single Projection
type columnListImpl []ColumnExpression

func (cl columnListImpl) Columns() []ColumnExpression {
	return cl
}

func (cl columnListImpl) fromImpl(subQuery SelectTable) Projection {
	newProjectionList := ProjectionList{}

	for _, column := range cl {
		newProjectionList = append(newProjectionList, column.fromImpl(subQuery))
	}

	return newProjectionList
}

func (cl columnListImpl) serializeForProjection(statement StatementType, out *SqlBuilder) error {
	projections := ColumnListToProjectionList(cl)

	err := SerializeProjectionList(statement, projections, out)

	if err != nil {
		return err
	}

	return nil
}

// dummy column interface implementation

// Name is placeholder for ColumnList to implement Column interface
func (cl columnListImpl) Name() string { return "" }

// TableName is placeholder for ColumnList to implement Column interface
func (cl columnListImpl) TableName() string                { return "" }
func (cl columnListImpl) SetTableName(name string)         {}
func (cl columnListImpl) SetSubQuery(subQuery SelectTable) {}
func (cl columnListImpl) DefaultAlias() string             { return "" }
