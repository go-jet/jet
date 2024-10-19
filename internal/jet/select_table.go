package jet

// SelectTable is interface for SELECT sub-queries
type SelectTable interface {
	SerializerHasProjections
	Alias() string
	AllColumns() ProjectionList
}

type selectTableImpl struct {
	Statement     SerializerHasProjections
	alias         string
	columnAliases []ColumnExpression
}

// NewSelectTable func
func NewSelectTable(selectStmt SerializerHasProjections, alias string, columnAliases []ColumnExpression) selectTableImpl {
	selectTable := selectTableImpl{
		Statement:     selectStmt,
		alias:         alias,
		columnAliases: columnAliases,
	}

	for _, column := range selectTable.columnAliases {
		column.setSubQuery(selectTable)
	}

	return selectTable
}

func (s selectTableImpl) projections() ProjectionList {
	return s.Statement.projections()
}

func (s selectTableImpl) Alias() string {
	return s.alias
}

func (s selectTableImpl) AllColumns() ProjectionList {
	if len(s.columnAliases) > 0 {
		return ColumnListToProjectionList(s.columnAliases)
	}

	projectionList := s.projections().fromImpl(s)
	return projectionList.(ProjectionList)
}

func (s selectTableImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	s.Statement.serialize(statement, out)

	out.WriteString("AS")
	out.WriteIdentifier(s.alias)

	if len(s.columnAliases) > 0 {
		out.WriteByte('(')
		SerializeColumnExpressionNames(s.columnAliases, out)
		out.WriteByte(')')
	}
}

// --------------------------------------

type lateralImpl struct {
	selectTableImpl
}

// NewLateral creates new lateral expression from select statement with alias
func NewLateral(selectStmt SerializerStatement, alias string) SelectTable {
	return lateralImpl{selectTableImpl: NewSelectTable(selectStmt, alias, nil)}
}

func (s lateralImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("LATERAL")
	s.Statement.serialize(statement, out)

	out.WriteString("AS")
	out.WriteIdentifier(s.alias)
}
