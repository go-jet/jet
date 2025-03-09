// Modeling of columns

package jet

import (
	"github.com/go-jet/jet/v2/internal/3rdparty/snaker"
)

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
func NewColumnImpl(name string, tableName string, parent ColumnExpression) *ColumnExpressionImpl {
	newColumn := &ColumnExpressionImpl{
		name:      name,
		tableName: tableName,
	}

	if parent != nil {
		newColumn.ExpressionInterfaceImpl.Parent = parent
	} else {
		newColumn.ExpressionInterfaceImpl.Parent = newColumn
	}

	return newColumn
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

func (c *ColumnExpressionImpl) serializeForOrderBy(statement StatementType, out *SQLBuilder) {
	if statement == SetStatementType {
		// set Statement (UNION, EXCEPT ...) can reference only select projections in order by clause
		out.WriteAlias(c.defaultAlias()) //always quote
		return
	}

	c.serialize(statement, out)
}

func (c *ColumnExpressionImpl) serializeForProjection(statement StatementType, out *SQLBuilder) {
	c.serialize(statement, out)

	out.WriteString("AS")

	out.WriteAlias(c.defaultAlias())
}

func (c *ColumnExpressionImpl) serializeForJsonObjEntry(statement StatementType, out *SQLBuilder) {
	out.WriteJsonObjKey(snaker.SnakeToCamel(c.name, false))
	c.Parent.serializeForJsonValue(statement, out)
}

func (c *ColumnExpressionImpl) serializeForRowToJsonProjection(statement StatementType, out *SQLBuilder) {
	c.Parent.serializeForJsonValue(statement, out)

	out.WriteString("AS")

	out.WriteAlias(snaker.SnakeToCamel(c.name, false))
}

func (c *ColumnExpressionImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {

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
