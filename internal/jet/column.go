// Modeling of columns

package jet

// Column is common column interface for all types of columns.
type Column interface {
	Name() string
	TableName() string

	setTableName(table string)
	setSubQuery(subQuery SelectTable)
	defaultAlias() string
}

// ColumnSerializer is interface for all serializable columns
type ColumnSerializer interface {
	Serializer
	Column
}

// ColumnExpression interface
type ColumnExpression interface {
	Column
	Expression
}

// ColumnExpressionImpl is base type for sql columns.
type ColumnExpressionImpl struct {
	ExpressionInterfaceImpl

	name      string
	tableName string

	subQuery SelectTable
}

// NewColumnImpl creates new ColumnExpressionImpl
func NewColumnImpl(name string, tableName string, parent ColumnExpression) ColumnExpressionImpl {
	bc := ColumnExpressionImpl{
		name:      name,
		tableName: tableName,
	}

	if parent != nil {
		bc.ExpressionInterfaceImpl.Parent = parent
	} else {
		bc.ExpressionInterfaceImpl.Parent = &bc
	}

	return bc
}

// Name returns name of the column
func (c *ColumnExpressionImpl) Name() string {
	return c.name
}

// TableName returns column table name
func (c *ColumnExpressionImpl) TableName() string {
	return c.tableName
}

func (c *ColumnExpressionImpl) setTableName(table string) {
	c.tableName = table
}

func (c *ColumnExpressionImpl) setSubQuery(subQuery SelectTable) {
	c.subQuery = subQuery
}

func (c *ColumnExpressionImpl) defaultAlias() string {
	if c.tableName != "" {
		return c.tableName + "." + c.name
	}

	return c.name
}

func (c *ColumnExpressionImpl) fromImpl(subQuery SelectTable) Projection {
	newColumn := NewColumnImpl(c.name, c.tableName, nil)
	newColumn.setSubQuery(subQuery)

	return &newColumn
}

func (c *ColumnExpressionImpl) serializeForOrderBy(statement StatementType, out *SQLBuilder) {
	if statement == SetStatementType {
		// set Statement (UNION, EXCEPT ...) can reference only select projections in order by clause
		out.WriteAlias(c.defaultAlias()) //always quote
		return
	}

	c.serialize(statement, out)
}

func (c ColumnExpressionImpl) serializeForProjection(statement StatementType, out *SQLBuilder) {
	c.serialize(statement, out)

	out.WriteString("AS")
	out.WriteAlias(c.defaultAlias())
}

func (c ColumnExpressionImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {

	if c.subQuery != nil {
		out.WriteIdentifier(c.subQuery.Alias())
		out.WriteByte('.')
		out.WriteIdentifier(c.defaultAlias())
	} else {
		if c.tableName != "" && !contains(options, ShortName) {
			out.WriteIdentifier(c.tableName)
			out.WriteByte('.')
		}

		out.WriteIdentifier(c.name)
	}
}
