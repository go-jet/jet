// Modeling of columns

package jet

type column interface {
	Name() string
	TableName() string

	setTableName(table string)
	setSubQuery(subQuery SelectTable)
	defaultAlias() string
}

// Column is common column interface for all types of columns.
type Column interface {
	Expression
	column
}

// The base type for real materialized columns.
type columnImpl struct {
	expressionInterfaceImpl
	noOpVisitorImpl

	name      string
	tableName string

	subQuery SelectTable
}

func newColumn(name string, tableName string, parent Column) columnImpl {
	bc := columnImpl{
		name:      name,
		tableName: tableName,
	}

	bc.expressionInterfaceImpl.parent = parent

	return bc
}

func (c *columnImpl) Name() string {
	return c.name
}

func (c *columnImpl) TableName() string {
	return c.tableName
}

func (c *columnImpl) setTableName(table string) {
	c.tableName = table
}

func (c *columnImpl) setSubQuery(subQuery SelectTable) {
	c.subQuery = subQuery
}

func (c *columnImpl) defaultAlias() string {
	if c.tableName != "" {
		return c.tableName + "." + c.name
	}

	return c.name
}

func (c *columnImpl) serializeForOrderBy(statement statementType, out *sqlBuilder) error {
	if statement == setStatement {
		// set Statement (UNION, EXCEPT ...) can reference only select projections in order by clause
		out.writeAlias(c.defaultAlias()) //always quote

		return nil
	}

	return c.serialize(statement, out)
}

func (c columnImpl) serializeForProjection(statement statementType, out *sqlBuilder) error {
	err := c.serialize(statement, out)

	if err != nil {
		return err
	}

	out.writeString("AS")
	out.writeAlias(c.defaultAlias())

	return nil
}

func (c columnImpl) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {

	if c.subQuery != nil {
		out.writeIdentifier(c.subQuery.Alias())
		out.writeByte('.')
		out.writeAlias(c.defaultAlias())
	} else {
		if c.tableName != "" {
			out.writeIdentifier(c.tableName)
			out.writeByte('.')
		}

		out.writeIdentifier(c.name)
	}

	return nil
}

//------------------------------------------------------//

// ColumnList is redefined type to support list of columns as single projection
type ColumnList []Column

// projection interface implementation
func (cl ColumnList) isProjectionType() {}

func (cl ColumnList) from(subQuery SelectTable) projection {
	newProjectionList := ProjectionList{}

	for _, column := range cl {
		newProjectionList = append(newProjectionList, column.from(subQuery))
	}

	return newProjectionList
}

func (cl ColumnList) serializeForProjection(statement statementType, out *sqlBuilder) error {
	projections := columnListToProjectionList(cl)

	err := serializeProjectionList(statement, projections, out)

	if err != nil {
		return err
	}

	return nil
}

// dummy column interface implementation

// Name is placeholder for ColumnList to implement Column interface
func (cl ColumnList) Name() string { return "" }

// TableName is placeholder for ColumnList to implement Column interface
func (cl ColumnList) TableName() string                { return "" }
func (cl ColumnList) setTableName(name string)         {}
func (cl ColumnList) setSubQuery(subQuery SelectTable) {}
func (cl ColumnList) defaultAlias() string             { return "" }
