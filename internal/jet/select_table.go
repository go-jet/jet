package jet

// SelectTable is interface for SELECT sub-queries
type SelectTable interface {
	Serializer
	Alias() string
	AllColumns() ProjectionList
}

type selectTableImpl struct {
	selectStmt StatementWithProjections
	alias      string

	projections ProjectionList
}

// NewSelectTable func
func NewSelectTable(selectStmt StatementWithProjections, alias string) SelectTable {
	selectTable := selectTableImpl{selectStmt: selectStmt, alias: alias}

	projectionList := selectStmt.projections().fromImpl(&selectTable)
	selectTable.projections = projectionList.(ProjectionList)

	return &selectTable
}

func (s *selectTableImpl) Alias() string {
	return s.alias
}

func (s *selectTableImpl) AllColumns() ProjectionList {
	return s.projections
}

func (s *selectTableImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if s == nil {
		panic("jet: expression table is nil. ")
	}

	s.selectStmt.serialize(statement, out)

	out.WriteString("AS")
	out.WriteIdentifier(s.alias)
}
